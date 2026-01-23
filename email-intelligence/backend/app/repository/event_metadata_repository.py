"""Repository helpers for event extraction metadata."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any

import json


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def list_messages_in_category(
    *,
    engine: Any,
    category: str,
    subcategory: str | None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """List candidate messages for event extraction.

    Args:
        engine: SQLAlchemy engine.
        category: Tier-1 category.
        subcategory: Tier-2 subcategory (optional).
        limit: Max rows.

    Returns:
        Rows with message_id, gmail_message_id, subject, from_domain, internal_date.
    """

    from sqlalchemy import text

    limit = max(1, min(int(limit), 5000))

    where = ["em.category = :category", "NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))"]
    params: dict[str, object] = {"category": str(category), "limit": limit}

    if subcategory is None:
        where.append("COALESCE(em.subcategory, '') = ''")
    else:
        where.append("em.subcategory = :subcategory")
        params["subcategory"] = str(subcategory)

    where_sql = " AND ".join(where)

    q = text(
        f"""
        SELECT
            em.id AS message_id,
            em.gmail_message_id AS gmail_message_id,
            em.subject AS subject,
            em.from_domain AS from_domain,
            em.internal_date AS internal_date
        FROM email_message em
        WHERE {where_sql}
        ORDER BY em.internal_date ASC, em.id ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, params).mappings().all()

    return [dict(r) for r in rows]


def list_messages_received_since(
    *,
    engine: Any,
    received_since: datetime,
    limit: int = 5000,
    include_trash: bool = False,
) -> list[dict[str, Any]]:
    """List candidate messages received since a given timestamp.

    This is useful for running event extraction over a recent time window.

    Args:
        engine: SQLAlchemy engine.
        received_since: Only include emails with internal_date >= this value.
        limit: Max rows.
        include_trash: If False, exclude messages with the TRASH label.

    Returns:
        Rows with message_id, gmail_message_id, subject, from_domain, internal_date.
    """

    from sqlalchemy import text

    limit = max(1, min(int(limit), 50_000))

    where = [
        "em.internal_date >= :received_since",
        # Avoid attempting Gmail fetches for synthetic dev/test messages.
        "em.gmail_message_id NOT LIKE 'fake-%'",
    ]
    if not include_trash:
        where.append("NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))")

    where_sql = " AND ".join(where)

    q = text(
        f"""
        SELECT
            em.id AS message_id,
            em.gmail_message_id AS gmail_message_id,
            em.subject AS subject,
            em.from_domain AS from_domain,
            em.internal_date AS internal_date
        FROM email_message em
        WHERE {where_sql}
        ORDER BY em.internal_date ASC, em.id ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"received_since": received_since, "limit": limit}).mappings().all()

    return [dict(r) for r in rows]


def upsert_message_event_metadata(
    *,
    engine: Any,
    message_id: int,
    status: str,
    event_name: str | None,
    event_type: str | None,
    event_date: date | None,
    start_time: time | None,
    end_time: time | None,
    timezone: str | None,
    end_time_inferred: bool,
    confidence: float | None,
    model: str | None,
    prompt_version: str | None,
    raw_json: dict | None,
    extracted_at: datetime | None = None,
    error: str | None = None,
) -> bool:
    """Insert or update message_event_metadata.

    Returns:
        True if inserted, False if updated.
    """

    from sqlalchemy import text

    if extracted_at is None:
        extracted_at = _now_utc()

    q = text(
        """
        INSERT INTO message_event_metadata (
            message_id,
            status,
            error,
            event_name,
            event_type,
            event_date,
            start_time,
            end_time,
            timezone,
            end_time_inferred,
            confidence,
            model,
            prompt_version,
            raw_json,
            extracted_at,
            updated_at
        )
        VALUES (
            :message_id,
            :status,
            :error,
            :event_name,
            :event_type,
            :event_date,
            :start_time,
            :end_time,
            :timezone,
            :end_time_inferred,
            :confidence,
            :model,
            :prompt_version,
            CAST(:raw_json AS JSONB),
            :extracted_at,
            NOW()
        )
        ON CONFLICT (message_id) DO UPDATE
        SET
            status = EXCLUDED.status,
            error = EXCLUDED.error,
            event_name = EXCLUDED.event_name,
            event_type = EXCLUDED.event_type,
            event_date = EXCLUDED.event_date,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            timezone = EXCLUDED.timezone,
            end_time_inferred = EXCLUDED.end_time_inferred,
            confidence = EXCLUDED.confidence,
            model = EXCLUDED.model,
            prompt_version = EXCLUDED.prompt_version,
            raw_json = EXCLUDED.raw_json,
            extracted_at = EXCLUDED.extracted_at,
            updated_at = NOW()
        RETURNING (xmax = 0) AS inserted
        """
    )

    payload = {
        "message_id": int(message_id),
        "status": str(status),
        "error": error,
        "event_name": event_name,
        "event_type": event_type,
        "event_date": event_date,
        "start_time": start_time,
        "end_time": end_time,
        "timezone": timezone,
        "end_time_inferred": bool(end_time_inferred),
        "confidence": confidence,
        "model": model,
        "prompt_version": prompt_version,
        "raw_json": None if raw_json is None else json.dumps(raw_json),
        "extracted_at": extracted_at,
    }

    with engine.begin() as conn:
        row = conn.execute(q, payload).mappings().first()

    return bool(row["inserted"]) if row else False


def list_future_events(
    *,
    engine: Any,
    limit: int = 200,
    include_hidden: bool = False,
) -> list[dict[str, Any]]:
    """List future event extractions for UI.

    Args:
        engine: SQLAlchemy engine.
        limit: Max rows.
        include_hidden: If True, include hidden/dismissed rows.

    Returns:
        Rows ordered by date/time with message_id and display fields.
    """

    from sqlalchemy import text

    limit = max(1, min(int(limit), 2000))

    where = [
        "mem.status = 'succeeded'",
        "mem.event_date IS NOT NULL",
        "mem.event_date >= CURRENT_DATE",
        "em.gmail_message_id NOT LIKE 'fake-%'",
        "NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))",
    ]
    if not include_hidden:
        where.append("mem.hidden_at IS NULL")

    where_sql = " AND ".join(where)

    q = text(
        f"""
        SELECT
            mem.message_id,
            mem.event_date,
            mem.start_time,
            mem.end_time,
            mem.end_time_inferred,
            mem.timezone,
            mem.event_type,
            mem.event_name,
            mem.calendar_event_id,
            mem.calendar_checked_at,
            mem.calendar_published_at,
            mem.hidden_at,
            em.subject,
            em.from_domain,
            em.internal_date
        FROM message_event_metadata mem
        JOIN email_message em ON em.id = mem.message_id
        WHERE {where_sql}
        ORDER BY mem.event_date ASC, mem.start_time ASC NULLS LAST, em.internal_date ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"limit": limit}).mappings().all()

    return [dict(r) for r in rows]


def hide_event(
    *,
    engine: Any,
    message_id: int,
) -> None:
    """Hide/dismiss an event so it no longer appears in the future events view."""

    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE message_event_metadata
                SET
                    -- NOTE: We enforce an event_type CHECK constraint as NOT VALID.
                    -- Postgres will still enforce it on *updated* rows, which means
                    -- legacy rows with event_type like 'other'/'travel' can fail
                    -- unrelated updates (like hiding). Normalize defensively here.
                    event_type = CASE
                        WHEN event_type IS NULL THEN NULL
                        WHEN event_type IN ('Theatre', 'Comedy', 'Opera', 'Ballet', 'Cinema', 'Social', 'Other')
                            THEN event_type
                        WHEN lower(event_type) IN ('theatre', 'comedy', 'opera', 'ballet', 'cinema', 'social')
                            THEN initcap(lower(event_type))
                        WHEN lower(event_type) = 'other'
                            THEN 'Other'
                        ELSE 'Other'
                    END,
                    hidden_at = NOW(),
                    updated_at = NOW()
                WHERE message_id = :mid
                """
            ),
            {"mid": int(message_id)},
        )


def unhide_event(
    *,
    engine: Any,
    message_id: int,
) -> None:
    """Unhide a previously hidden/dismissed event."""

    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE message_event_metadata
                SET
                    -- Keep legacy rows compliant with the NOT VALID event_type CHECK.
                    event_type = CASE
                        WHEN event_type IS NULL THEN NULL
                        WHEN event_type IN ('Theatre', 'Comedy', 'Opera', 'Ballet', 'Cinema', 'Social', 'Other')
                            THEN event_type
                        WHEN lower(event_type) IN ('theatre', 'comedy', 'opera', 'ballet', 'cinema', 'social')
                            THEN initcap(lower(event_type))
                        WHEN lower(event_type) = 'other'
                            THEN 'Other'
                        ELSE 'Other'
                    END,
                    hidden_at = NULL,
                    updated_at = NOW()
                WHERE message_id = :mid
                """
            ),
            {"mid": int(message_id)},
        )


def set_calendar_status(
    *,
    engine: Any,
    message_id: int,
    calendar_ical_uid: str | None,
    calendar_event_id: str | None,
    checked_at_utc: datetime | None,
    published_at_utc: datetime | None = None,
) -> None:
    """Update cached calendar status fields for a message_event_metadata row."""

    from sqlalchemy import text

    q = text(
        """
        UPDATE message_event_metadata
        SET
            -- See note in hide_event(): keep legacy rows compliant with the NOT VALID event_type CHECK.
            event_type = CASE
                WHEN event_type IS NULL THEN NULL
                WHEN event_type IN ('Theatre', 'Comedy', 'Opera', 'Ballet', 'Cinema', 'Social', 'Other')
                    THEN event_type
                WHEN lower(event_type) IN ('theatre', 'comedy', 'opera', 'ballet', 'cinema', 'social')
                    THEN initcap(lower(event_type))
                WHEN lower(event_type) = 'other'
                    THEN 'Other'
                ELSE 'Other'
            END,
            calendar_ical_uid = COALESCE(:calendar_ical_uid, calendar_ical_uid),
            calendar_event_id = :calendar_event_id,
            calendar_checked_at = :checked_at,
            calendar_published_at = COALESCE(:published_at, calendar_published_at),
            updated_at = NOW()
        WHERE message_id = :mid
        """
    )

    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "mid": int(message_id),
                "calendar_ical_uid": calendar_ical_uid,
                "calendar_event_id": calendar_event_id,
                "checked_at": checked_at_utc,
                "published_at": published_at_utc,
            },
        )


def get_event_row_for_message(
    *,
    engine: Any,
    message_id: int,
) -> dict[str, Any] | None:
    """Fetch event + email metadata for one message_id."""

    from sqlalchemy import text

    q = text(
        """
        SELECT
            mem.message_id,
            mem.status,
            mem.event_date,
            mem.start_time,
            mem.end_time,
            mem.end_time_inferred,
            mem.timezone,
            mem.event_type,
            mem.event_name,
            mem.calendar_event_id,
            mem.calendar_ical_uid,
            em.subject,
            em.from_domain,
            em.internal_date
        FROM message_event_metadata mem
        JOIN email_message em ON em.id = mem.message_id
        WHERE mem.message_id = :mid
        """
    )

    with engine.begin() as conn:
        row = conn.execute(q, {"mid": int(message_id)}).mappings().first()
    return dict(row) if row else None
