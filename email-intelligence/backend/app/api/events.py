"""Events API.

Provides a small UI-focused surface for the extracted future events list and
Google Calendar publish/check actions.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException

from app.api.models import CalendarCheckResponse
from app.api.models import CalendarPublishResponse
from app.api.models import FutureEventsResponse
from app.api.models import HideEventResponse
from app.db.postgres import engine
from app.google.calendar_client import get_calendar_service_from_files
from app.repository.event_metadata_repository import get_event_row_for_message
from app.repository.event_metadata_repository import hide_event
from app.repository.event_metadata_repository import list_future_events
from app.repository.event_metadata_repository import set_calendar_status
from app.repository.event_metadata_repository import unhide_event
from app.settings import Settings

router = APIRouter(prefix="/api/events", tags=["events"])


_CALENDAR_CHECK_TTL = timedelta(hours=24)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_tz_name(tz_name: str | None, *, fallback: str) -> str:
    raw = (tz_name or "").strip()
    return raw or fallback or "UTC"


def _ical_uid_for_message(message_id: int) -> str:
    # Deterministic + stable so we can de-dupe publishes (and check existence) safely.
    # iCalUID must be globally unique; the domain doesn't need to exist.
    return f"email-intel-{int(message_id)}@email-manager-agent.local"


def _sync_calendar_status_for_rows(*, rows: list[dict[str, Any]]) -> None:
    """Best-effort: fill calendar_checked_at/calendar_event_id for future events.

    We do this efficiently by listing *all* calendar events for the date window covering the
    rows in a small number of Google Calendar API calls (paging), then matching on iCalUID.

    This is intentionally best-effort and must never break the /future endpoint; if calendar
    auth isn't configured or calls fail, we simply return without updating.
    """

    if not rows:
        return

    # Only check rows that have never been checked or are stale.
    now = _now_utc()
    to_check: list[dict[str, Any]] = []
    for r in rows:
        checked_at: datetime | None = r.get("calendar_checked_at")
        if checked_at is None or (now - checked_at) > _CALENDAR_CHECK_TTL:
            to_check.append(r)

    if not to_check:
        return

    settings = Settings()

    # If Calendar credentials/tokens are not available, silently skip.
    # (We must not trigger interactive auth from a GET endpoint.)
    creds_path = Path(settings.calendar_credentials_path)
    token_path = Path(settings.calendar_token_path)
    if not creds_path.exists() or not token_path.exists():
        return

    # Build a date range that covers the returned rows. Clamp to ~1 year so we don't
    # accidentally scan a multi-year window if a far-future event slips into the list.
    min_date = min((r.get("event_date") for r in rows if r.get("event_date") is not None), default=None)
    max_date = max((r.get("event_date") for r in rows if r.get("event_date") is not None), default=None)
    if min_date is None or max_date is None:
        return

    horizon = date.today() + timedelta(days=366)
    if max_date > horizon:
        max_date = horizon

    # RFC3339 timestamps; include the full day range.
    time_min = datetime.combine(min_date, time.min).replace(tzinfo=timezone.utc)
    time_max = datetime.combine(max_date + timedelta(days=1), time.min).replace(tzinfo=timezone.utc)

    # Gather iCalUIDs we care about.
    wanted_uids: dict[int, str] = {}
    for r in to_check:
        mid = int(r.get("message_id"))
        uid = (r.get("calendar_ical_uid") or "").strip() or _ical_uid_for_message(mid)
        wanted_uids[mid] = uid

    try:
        service = get_calendar_service_from_files(
            credentials_path=settings.calendar_credentials_path,
            token_path=settings.calendar_token_path,
            auth_mode=settings.calendar_auth_mode,
            # Never trigger interactive auth for background auto-checks.
            allow_interactive=False,
        )

        # List events in the window and build a map from iCalUID -> calendar event id.
        uid_to_event_id: dict[str, str] = {}
        page_token: str | None = None
        while True:
            resp = (
                service.events()
                .list(
                    calendarId=settings.calendar_id,
                    timeMin=time_min.isoformat().replace("+00:00", "Z"),
                    timeMax=time_max.isoformat().replace("+00:00", "Z"),
                    singleEvents=True,
                    showDeleted=False,
                    maxResults=2500,
                    pageToken=page_token,
                )
                .execute()
            )
            items = resp.get("items") or []
            for it in items:
                uid = (it.get("iCalUID") or "").strip()
                eid = (it.get("id") or "").strip()
                if uid and eid:
                    uid_to_event_id[uid] = eid

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        # Persist cache results.
        for mid, uid in wanted_uids.items():
            set_calendar_status(
                engine=engine,
                message_id=mid,
                calendar_ical_uid=uid,
                calendar_event_id=uid_to_event_id.get(uid),
                checked_at_utc=now,
                published_at_utc=None,
            )

    except Exception:
        # Best-effort only: do not break the future events view if Calendar isn't available.
        return


def _build_calendar_event_body(
    *,
    row: dict[str, Any],
    calendar_ical_uid: str,
    default_tz_name: str,
) -> dict[str, Any]:
    event_date: date | None = row.get("event_date")
    start_time: time | None = row.get("start_time")
    end_time: time | None = row.get("end_time")

    if event_date is None:
        raise HTTPException(status_code=400, detail="Event has no date")

    tz_name = _normalize_tz_name(row.get("timezone"), fallback=default_tz_name)

    summary = (row.get("event_name") or "").strip() or (row.get("subject") or "").strip() or "Event"

    desc_parts: list[str] = []
    if row.get("subject"):
        desc_parts.append(f"Email subject: {row['subject']}")
    if row.get("from_domain"):
        desc_parts.append(f"From domain: {row['from_domain']}")
    desc_parts.append(f"Email Intelligence message_id: {row.get('message_id')}")

    body: dict[str, Any] = {
        "summary": summary,
        "description": "\n".join(desc_parts),
        "iCalUID": calendar_ical_uid,
        # Ensure a consistent reminder regardless of a user's default calendar settings.
        # We explicitly disable defaults to avoid common 10-minute popups.
        # 1440 minutes = 1 day.
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "email", "minutes": 1440},
            ],
        },
    }

    if start_time is None:
        # All-day event
        body["start"] = {"date": event_date.isoformat()}
        body["end"] = {"date": (event_date + timedelta(days=1)).isoformat()}
        return body

    # Timed event
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo(default_tz_name or "UTC")
        tz_name = getattr(tz, "key", "UTC")

    start_dt = datetime.combine(event_date, start_time).replace(tzinfo=tz)

    if end_time is None:
        # If we don't have an end time, use a conservative default duration.
        end_dt = start_dt + timedelta(hours=2)
    else:
        end_dt = datetime.combine(event_date, end_time).replace(tzinfo=tz)
        if end_dt <= start_dt:
            # Defensive: if it looks like it crosses midnight, bump end date.
            end_dt = end_dt + timedelta(days=1)

    body["start"] = {"dateTime": start_dt.isoformat(), "timeZone": tz_name}
    body["end"] = {"dateTime": end_dt.isoformat(), "timeZone": tz_name}
    return body


@router.get("/future", response_model=FutureEventsResponse)
def get_future_events(
    limit: int = 200,
    include_hidden: bool = False,
    auto_calendar_check: bool = True,
) -> FutureEventsResponse:
    rows = list_future_events(engine=engine, limit=limit, include_hidden=include_hidden)

    if auto_calendar_check:
        _sync_calendar_status_for_rows(rows=rows)
        # Re-read to return fresh cached calendar status to the UI.
        rows = list_future_events(engine=engine, limit=limit, include_hidden=include_hidden)

    # Pydantic response model handles conversion.
    return FutureEventsResponse(generated_at=_now_utc(), events=rows)  # type: ignore[arg-type]


@router.post("/{message_id}/hide", response_model=HideEventResponse)
def post_hide_event(message_id: int) -> HideEventResponse:
    hide_event(engine=engine, message_id=message_id)
    return HideEventResponse(message_id=int(message_id), hidden=True, hidden_at=_now_utc())


@router.post("/{message_id}/unhide", response_model=HideEventResponse)
def post_unhide_event(message_id: int) -> HideEventResponse:
    unhide_event(engine=engine, message_id=message_id)
    return HideEventResponse(message_id=int(message_id), hidden=False, hidden_at=None)


@router.post("/{message_id}/calendar/check", response_model=CalendarCheckResponse)
def post_calendar_check(message_id: int) -> CalendarCheckResponse:
    settings = Settings()

    row = get_event_row_for_message(engine=engine, message_id=message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")

    calendar_ical_uid = (row.get("calendar_ical_uid") or "").strip() or _ical_uid_for_message(message_id)

    try:
        service = get_calendar_service_from_files(
            credentials_path=settings.calendar_credentials_path,
            token_path=settings.calendar_token_path,
            auth_mode=settings.calendar_auth_mode,
            allow_interactive=settings.calendar_allow_interactive,
        )

        resp = (
            service.events()
            .list(
                calendarId=settings.calendar_id,
                iCalUID=calendar_ical_uid,
                maxResults=1,
                singleEvents=True,
                showDeleted=False,
            )
            .execute()
        )
        items = resp.get("items") or []
        found = items[0] if items else None
        event_id = found.get("id") if found else None

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Calendar check failed: {e}")

    checked_at = _now_utc()
    set_calendar_status(
        engine=engine,
        message_id=message_id,
        calendar_ical_uid=calendar_ical_uid,
        calendar_event_id=event_id,
        checked_at_utc=checked_at,
        published_at_utc=None,
    )

    return CalendarCheckResponse(
        message_id=int(message_id),
        calendar_ical_uid=calendar_ical_uid,
        exists=event_id is not None,
        calendar_event_id=event_id,
        calendar_checked_at=checked_at,
    )


@router.post("/{message_id}/calendar/publish", response_model=CalendarPublishResponse)
def post_calendar_publish(message_id: int) -> CalendarPublishResponse:
    settings = Settings()

    row = get_event_row_for_message(engine=engine, message_id=message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")

    if row.get("status") != "succeeded":
        raise HTTPException(status_code=400, detail="Event is not in succeeded state")

    event_date_val: date | None = row.get("event_date")
    if event_date_val is None:
        raise HTTPException(status_code=400, detail="Event has no date")

    calendar_ical_uid = (row.get("calendar_ical_uid") or "").strip() or _ical_uid_for_message(message_id)

    try:
        service = get_calendar_service_from_files(
            credentials_path=settings.calendar_credentials_path,
            token_path=settings.calendar_token_path,
            auth_mode=settings.calendar_auth_mode,
            allow_interactive=settings.calendar_allow_interactive,
        )

        # First, check if it already exists.
        resp = (
            service.events()
            .list(
                calendarId=settings.calendar_id,
                iCalUID=calendar_ical_uid,
                maxResults=1,
                singleEvents=True,
                showDeleted=False,
            )
            .execute()
        )
        items = resp.get("items") or []
        found = items[0] if items else None
        if found and found.get("id"):
            checked_at = _now_utc()
            set_calendar_status(
                engine=engine,
                message_id=message_id,
                calendar_ical_uid=calendar_ical_uid,
                calendar_event_id=found.get("id"),
                checked_at_utc=checked_at,
                published_at_utc=None,
            )
            return CalendarPublishResponse(
                message_id=int(message_id),
                calendar_ical_uid=calendar_ical_uid,
                already_existed=True,
                calendar_event_id=str(found.get("id")),
                calendar_published_at=None,
            )

        body = _build_calendar_event_body(
            row=row,
            calendar_ical_uid=calendar_ical_uid,
            default_tz_name=settings.calendar_default_timezone,
        )

        created = (
            service.events()
            .insert(
                calendarId=settings.calendar_id,
                body=body,
                sendUpdates="none",
            )
            .execute()
        )

        event_id = created.get("id")
        if not event_id:
            raise RuntimeError("Calendar insert returned no event id")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Calendar publish failed: {e}")

    now = _now_utc()
    set_calendar_status(
        engine=engine,
        message_id=message_id,
        calendar_ical_uid=calendar_ical_uid,
        calendar_event_id=str(event_id),
        checked_at_utc=now,
        published_at_utc=now,
    )

    return CalendarPublishResponse(
        message_id=int(message_id),
        calendar_ical_uid=calendar_ical_uid,
        already_existed=False,
        calendar_event_id=str(event_id),
        calendar_published_at=now,
    )
