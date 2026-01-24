"""Maintenance orchestration for the Email Intelligence pipeline."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.ingestion.metadata_ingestion import ingest_metadata
from app.labeling.incremental_pipeline import label_unlabelled_individual
from app.repository.email_query_repository import count_unlabelled_since
from app.repository.event_metadata_repository import (
    list_unprocessed_messages_in_category_since,
    upsert_message_event_metadata,
)
from app.repository.payment_metadata_repository import (
    list_unprocessed_messages_in_category_any_subcategory,
    list_unprocessed_messages_received_since,
    upsert_message_payment_metadata,
)
from app.repository.pipeline_kv_repository import (
    get_checkpoint_internal_date,
    get_retention_default_days,
    set_checkpoint_internal_date,
)
from app.repository.retention_archive_repository import (
    count_pending_outbox,
    fetch_pending_batch,
    mark_outbox_failed,
    mark_outbox_succeeded,
    plan_archive_outbox,
)
from app.repository.taxonomy_admin_repository import GMAIL_ARCHIVED_LABEL_NAME
from app.settings import Settings
from app.vector.qdrant import ensure_collection


ProgressCallback = Callable[..., None]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _call_progress(
    progress_cb: ProgressCallback | None,
    *,
    phase: str,
    message: str | None = None,
    processed: int | None = None,
    total: int | None = None,
    inserted: int | None = None,
    skipped_existing: int | None = None,
    failed: int | None = None,
) -> None:
    if not progress_cb:
        return
    progress_cb(
        phase=phase,
        message=message,
        processed=processed,
        total=total,
        inserted=inserted,
        skipped_existing=skipped_existing,
        failed=failed,
    )


def _push_label_outbox(
    *,
    engine: Any,
    settings: Settings,
    service,
    batch_size: int,
    progress_cb: ProgressCallback | None,
) -> tuple[int, int, int]:
    """Drain label_push_outbox and apply Gmail labels."""

    from sqlalchemy import text

    from app.gmail.client import modify_message_labels
    from googleapiclient.errors import HttpError

    q_total = text("SELECT COUNT(*) FROM label_push_outbox WHERE processed_at IS NULL")
    q_fetch = text(
        """
        SELECT o.id, o.message_id, em.gmail_message_id
        FROM label_push_outbox o
        JOIN email_message em ON em.id = o.message_id
        WHERE o.processed_at IS NULL
          AND em.gmail_message_id IS NOT NULL
        ORDER BY o.created_at ASC
        LIMIT :limit
        """
    )

    q_tids = text(
        """
        SELECT mtl.taxonomy_label_id
        FROM message_taxonomy_label mtl
        JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
        WHERE mtl.message_id = :mid AND tl.is_active = TRUE
        """
    )

    q_label_rows = text(
        """
        SELECT id, gmail_label_id
        FROM taxonomy_label
        WHERE id = ANY(:ids)
        """
    )

    q_mark_ok = text(
        """
        UPDATE label_push_outbox
        SET processed_at = NOW(), error = NULL
        WHERE id = :id
        """
    )

    q_mark_err = text(
        """
        UPDATE label_push_outbox
        SET processed_at = NOW(), error = :error
        WHERE id = :id
        """
    )

    with engine.begin() as conn:
        total = conn.execute(q_total).scalar()

    _call_progress(
        progress_cb,
        phase="maintenance_label_push",
        total=int(total or 0),
        message=f"Starting label outbox push (~{int(total or 0)} message(s))",
    )

    processed = 0
    succeeded = 0
    failed = 0
    batch_num = 0

    while True:
        with engine.begin() as conn:
            outbox = conn.execute(q_fetch, {"limit": int(batch_size)}).mappings().all()

        if not outbox:
            break

        batch_num += 1
        for o in outbox:
            outbox_id = int(o["id"])
            message_id = int(o["message_id"])
            gmail_message_id = str(o["gmail_message_id"])

            processed += 1
            try:
                with engine.begin() as conn:
                    tids = conn.execute(q_tids, {"mid": int(message_id)}).fetchall()
                    tset = [int(r[0]) for r in tids]
                    label_rows = conn.execute(q_label_rows, {"ids": list(tset)}).fetchall()
                    taxonomy_to_gmail = {
                        int(r[0]): str(r[1])
                        for r in label_rows
                        if r[1] is not None and str(r[1]).strip()
                    }

                add_ids = [taxonomy_to_gmail.get(tid) for tid in tset]
                add_ids = [x for x in add_ids if x]
                if not add_ids:
                    raise RuntimeError("missing gmail label mapping for message")

                modify_message_labels(
                    service,
                    message_id=gmail_message_id,
                    add_label_ids=add_ids,
                    remove_label_ids=None,
                    user_id=settings.gmail_user_id,
                )

                with engine.begin() as conn:
                    conn.execute(q_mark_ok, {"id": int(outbox_id)})

                succeeded += 1
            except HttpError as he:
                status = getattr(getattr(he, "resp", None), "status", None)
                if status in {429, 500, 502, 503, 504}:
                    try:
                        time.sleep(0.25)
                        modify_message_labels(
                            service,
                            message_id=gmail_message_id,
                            add_label_ids=add_ids,
                            remove_label_ids=None,
                            user_id=settings.gmail_user_id,
                        )
                        with engine.begin() as conn:
                            conn.execute(q_mark_ok, {"id": int(outbox_id)})
                        succeeded += 1
                        continue
                    except Exception as e:
                        failed += 1
                        with engine.begin() as conn:
                            conn.execute(q_mark_err, {"id": int(outbox_id), "error": str(e)[:5000]})
                        continue

                failed += 1
                with engine.begin() as conn:
                    conn.execute(q_mark_err, {"id": int(outbox_id), "error": str(he)[:5000]})
            except Exception as e:  # noqa: BLE001
                failed += 1
                with engine.begin() as conn:
                    conn.execute(q_mark_err, {"id": int(outbox_id), "error": str(e)[:5000]})

            if processed % 50 == 0:
                _call_progress(
                    progress_cb,
                    phase="maintenance_label_push",
                    processed=processed,
                    inserted=succeeded,
                    failed=failed,
                    message=(
                        f"Pushing label outbox… batch {batch_num} "
                        f"(ok {succeeded}, failed {failed})"
                    ),
                )

        _call_progress(
            progress_cb,
            phase="maintenance_label_push",
            processed=processed,
            inserted=succeeded,
            failed=failed,
            message=f"Completed batch {batch_num} (ok {succeeded}, failed {failed})",
        )
        time.sleep(0.05)

    _call_progress(
        progress_cb,
        phase="maintenance_label_push",
        processed=processed,
        inserted=succeeded,
        failed=failed,
        message=(
            f"Finished label outbox push: processed {processed}, "
            f"ok {succeeded}, failed {failed}"
        ),
    )

    return processed, succeeded, failed


def _push_archive_outbox(
    *,
    engine: Any,
    settings: Settings,
    service,
    batch_size: int,
    progress_cb: ProgressCallback | None,
) -> tuple[int, int, int]:
    """Drain archive_push_outbox and apply archive marker labels."""

    from app.gmail.client import create_label, label_name_to_id, modify_message_labels
    from googleapiclient.errors import HttpError

    candidate_label_names = [
        GMAIL_ARCHIVED_LABEL_NAME,
        "Email-Archive",
        "Archive (marker)",
    ]

    total = count_pending_outbox(engine=engine)
    _call_progress(
        progress_cb,
        phase="maintenance_archive_push",
        total=int(total),
        message=f"Starting archive push for {int(total)} message(s)",
    )

    name_to_id = label_name_to_id(service, user_id=settings.gmail_user_id)
    archived_label_id: str | None = None
    for nm in ["Archive", "Archived", *candidate_label_names]:
        if nm in name_to_id:
            archived_label_id = str(name_to_id[nm])
            break

    if not archived_label_id:
        last_err: Exception | None = None
        for nm in candidate_label_names:
            try:
                created = create_label(service, name=nm, user_id=settings.gmail_user_id)
                archived_label_id = str(created.get("id"))
                break
            except HttpError as he:
                last_err = he
                continue
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue

        if not archived_label_id:
            raise RuntimeError(f"Failed to create archive marker label. Last error: {last_err}")

    processed = 0
    succeeded = 0
    failed = 0
    batch_num = 0

    while True:
        batch = fetch_pending_batch(engine=engine, limit=int(batch_size))
        if not batch:
            break

        batch_num += 1
        for row in batch:
            processed += 1
            try:
                if not archived_label_id:
                    raise RuntimeError("archive label id not available")

                modify_message_labels(
                    service,
                    message_id=row.gmail_message_id,
                    add_label_ids=[str(archived_label_id)],
                    remove_label_ids=None,
                    user_id=settings.gmail_user_id,
                )
                mark_outbox_succeeded(engine=engine, outbox_id=row.id, message_id=row.message_id)
                succeeded += 1
            except HttpError as he:
                status = getattr(getattr(he, "resp", None), "status", None)
                if status in {429, 500, 502, 503, 504}:
                    try:
                        time.sleep(0.25)
                        modify_message_labels(
                            service,
                            message_id=row.gmail_message_id,
                            add_label_ids=[str(archived_label_id)],
                            remove_label_ids=None,
                            user_id=settings.gmail_user_id,
                        )
                        mark_outbox_succeeded(
                            engine=engine,
                            outbox_id=row.id,
                            message_id=row.message_id,
                        )
                        succeeded += 1
                        continue
                    except Exception as e:  # noqa: BLE001
                        failed += 1
                        mark_outbox_failed(engine=engine, outbox_id=row.id, error=str(e))
                        continue

                failed += 1
                mark_outbox_failed(engine=engine, outbox_id=row.id, error=str(he))
            except Exception as e:  # noqa: BLE001
                failed += 1
                mark_outbox_failed(engine=engine, outbox_id=row.id, error=str(e))

            if processed % 50 == 0:
                _call_progress(
                    progress_cb,
                    phase="maintenance_archive_push",
                    processed=processed,
                    inserted=succeeded,
                    failed=failed,
                    message=(
                        f"Applying archive label… batch {batch_num} "
                        f"(ok {succeeded}, failed {failed})"
                    ),
                )

        _call_progress(
            progress_cb,
            phase="maintenance_archive_push",
            processed=processed,
            inserted=succeeded,
            failed=failed,
            message=f"Completed batch {batch_num} (ok {succeeded}, failed {failed})",
        )
        time.sleep(0.05)

    _call_progress(
        progress_cb,
        phase="maintenance_archive_push",
        processed=processed,
        inserted=succeeded,
        failed=failed,
        message=(
            f"Finished archive push: processed {processed}, "
            f"ok {succeeded}, failed {failed}"
        ),
    )

    return processed, succeeded, failed


def _cleanup_inbox(
    *,
    engine: Any,
    settings: Settings,
    service,
    cutoff: datetime,
    batch_size: int,
    progress_cb: ProgressCallback | None,
) -> tuple[int, int, int]:
    """Remove the INBOX label for messages older than cutoff."""

    from sqlalchemy import text

    from app.gmail.client import modify_message_labels
    from googleapiclient.errors import HttpError

    q_fetch = text(
        """
        SELECT em.id AS message_id, em.gmail_message_id AS gmail_message_id
        FROM email_message em
        WHERE em.gmail_message_id IS NOT NULL
          AND em.inbox_removed_at IS NULL
          AND em.internal_date <= :cutoff
          AND 'INBOX' = ANY(COALESCE(em.label_ids, ARRAY[]::text[]))
          AND NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))
        ORDER BY em.internal_date ASC, em.id ASC
        LIMIT :limit
        """
    )

    q_update = text(
        """
        UPDATE email_message
        SET
            inbox_removed_at = NOW(),
            label_ids = array_remove(label_ids, 'INBOX')
        WHERE id = :message_id
        """
    )

    processed = 0
    succeeded = 0
    failed = 0
    batch_num = 0

    while True:
        with engine.begin() as conn:
            rows = (
                conn.execute(q_fetch, {"cutoff": cutoff, "limit": int(batch_size)})
                .mappings()
                .all()
            )

        if not rows:
            break

        batch_num += 1
        for row in rows:
            processed += 1
            mid = int(row["message_id"])
            gmail_id = str(row["gmail_message_id"])
            try:
                modify_message_labels(
                    service,
                    message_id=gmail_id,
                    add_label_ids=None,
                    remove_label_ids=["INBOX"],
                    user_id=settings.gmail_user_id,
                )
                with engine.begin() as conn:
                    conn.execute(q_update, {"message_id": mid})
                succeeded += 1
            except HttpError as he:
                status = getattr(getattr(he, "resp", None), "status", None)
                if status in {429, 500, 502, 503, 504}:
                    try:
                        time.sleep(0.25)
                        modify_message_labels(
                            service,
                            message_id=gmail_id,
                            add_label_ids=None,
                            remove_label_ids=["INBOX"],
                            user_id=settings.gmail_user_id,
                        )
                        with engine.begin() as conn:
                            conn.execute(q_update, {"message_id": mid})
                        succeeded += 1
                        continue
                    except Exception:
                        failed += 1
                        continue

                failed += 1
            except Exception:  # noqa: BLE001
                failed += 1

            if processed % 50 == 0:
                _call_progress(
                    progress_cb,
                    phase="maintenance_inbox_cleanup",
                    processed=processed,
                    inserted=succeeded,
                    failed=failed,
                    message=(
                        f"Inbox aging… batch {batch_num} "
                        f"(ok {succeeded}, failed {failed})"
                    ),
                )

        _call_progress(
            progress_cb,
            phase="maintenance_inbox_cleanup",
            processed=processed,
            inserted=succeeded,
            failed=failed,
            message=f"Completed batch {batch_num} (ok {succeeded}, failed {failed})",
        )
        time.sleep(0.05)

    _call_progress(
        progress_cb,
        phase="maintenance_inbox_cleanup",
        processed=processed,
        inserted=succeeded,
        failed=failed,
        message=(
            f"Finished inbox aging: processed {processed}, "
            f"ok {succeeded}, failed {failed}"
        ),
    )

    return processed, succeeded, failed


def run_maintenance(
    *,
    engine: Any,
    settings: Settings | None = None,
    inbox_cleanup_days: int | None = None,
    label_threshold: int | None = None,
    fallback_days: int | None = None,
    allow_interactive: bool = False,
    progress_cb: ProgressCallback | None = None,
) -> None:
    """Run the maintenance pipeline in one pass.

    Steps:
      1) Incremental metadata ingestion
      2) Auto labeling (incremental vs cluster)
      3) Push label outbox to Gmail
      4) Retention plan + archive push
      5) Inbox aging (remove INBOX label after N days)
      6) Event extraction for unprocessed messages
      7) Payment extraction for unprocessed messages

    Args:
        engine: SQLAlchemy engine.
        settings: Optional Settings override.
        inbox_cleanup_days: Remove INBOX for messages older than this many days.
        label_threshold: Auto-label threshold (switches between incremental vs cluster).
        fallback_days: If no ingestion checkpoint exists, use this recent window for extraction.
        allow_interactive: Allow interactive OAuth flows if tokens are missing.
        progress_cb: Optional callback for job progress updates.
    """

    from app.analysis.events.extractor import extract_event_from_email
    from app.analysis.events.prompt import PROMPT_VERSION as EVENT_PROMPT_VERSION
    from app.analysis.payments.extractor import extract_payment_from_email
    from app.analysis.payments.prompt import PROMPT_VERSION as PAYMENT_PROMPT_VERSION
    from app.gmail.client import (
        GMAIL_SCOPE_MODIFY,
        get_gmail_service_from_files,
        get_message_body_text,
    )

    if settings is None:
        settings = Settings()

    inbox_cleanup_days = int(inbox_cleanup_days or settings.inbox_cleanup_days)
    label_threshold = int(label_threshold or settings.maintenance_label_threshold)
    fallback_days = int(fallback_days or settings.maintenance_fallback_days)

    ensure_collection()

    checkpoint_before = get_checkpoint_internal_date(engine)
    if checkpoint_before is None:
        cutoff = _now_utc() - timedelta(days=fallback_days)
        set_checkpoint_internal_date(engine, cutoff)
        _call_progress(
            progress_cb,
            phase="maintenance_ingest",
            message=(
                "No ingest checkpoint found; limiting ingest to recent window "
                f"(since {cutoff.date().isoformat()})"
            ),
        )
    else:
        cutoff = checkpoint_before
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)

    service = get_gmail_service_from_files(
        credentials_path=settings.gmail_credentials_path,
        token_path=settings.gmail_token_path,
        scopes=[GMAIL_SCOPE_MODIFY],
        auth_mode=settings.gmail_auth_mode,
        allow_interactive=allow_interactive,
    )

    _call_progress(progress_cb, phase="maintenance_ingest", message="Starting metadata ingest")

    def ingest_hook(*, processed: int, skipped: int, failed: int, message: str | None):
        _call_progress(
            progress_cb,
            phase="maintenance_ingest",
            processed=processed,
            inserted=processed,
            skipped_existing=skipped,
            failed=failed,
            message=message,
        )

    ingest_metadata(
        engine=engine,
        service=service,
        user_id=settings.gmail_user_id,
        page_size=settings.gmail_page_size,
        max_messages=None,
        progress_hook=ingest_hook,
    )

    total_unlabelled = count_unlabelled_since(engine, received_since=cutoff)
    _call_progress(
        progress_cb,
        phase="maintenance_label",
        total=int(total_unlabelled),
        message=(
            f"Auto-label check: {int(total_unlabelled)} unlabelled "
            f"since {cutoff.date().isoformat()}"
        ),
    )

    if total_unlabelled > 0:
        _call_progress(
            progress_cb,
            phase="maintenance_label",
            message=(
                f"Auto: labeling unlabelled emails received since "
                f"{cutoff.date().isoformat()}"
            ),
        )

        def label_hook(
            *,
            emails_processed: int,
            emails_labeled: int,
            emails_failed: int,
            message: str | None,
        ):
            _call_progress(
                progress_cb,
                phase="maintenance_label",
                processed=emails_labeled,
                inserted=emails_labeled,
                failed=emails_failed,
                message=message,
            )

        label_unlabelled_individual(
            engine=engine,
            service=service,
            user_id=settings.gmail_user_id,
            similarity_threshold=settings.similarity_threshold,
            label_version=settings.label_version,
            ollama_host=settings.ollama_host,
            ollama_model=settings.ollama_model,
            received_since=cutoff,
            max_emails=None,
            progress_hook=label_hook,
        )

    _push_label_outbox(
        engine=engine,
        settings=settings,
        service=service,
        batch_size=250,
        progress_cb=progress_cb,
    )

    default_days = get_retention_default_days(engine)
    planned = plan_archive_outbox(engine=engine, default_days=default_days)
    _call_progress(
        progress_cb,
        phase="maintenance_retention_plan",
        inserted=planned,
        message=f"Retention plan: queued {planned} message(s) for archive",
    )

    _push_archive_outbox(
        engine=engine,
        settings=settings,
        service=service,
        batch_size=200,
        progress_cb=progress_cb,
    )

    cleanup_cutoff = _now_utc() - timedelta(days=max(1, inbox_cleanup_days))
    _cleanup_inbox(
        engine=engine,
        settings=settings,
        service=service,
        cutoff=cleanup_cutoff,
        batch_size=200,
        progress_cb=progress_cb,
    )

    if not settings.ollama_host:
        _call_progress(
            progress_cb,
            phase="maintenance_event_extract",
            message="Skipping event extraction: Ollama not configured",
        )
    else:
        event_rows = list_unprocessed_messages_in_category_since(
            engine=engine,
            category="Financial",
            subcategory="Tickets & Bookings",
            received_since=cutoff,
            limit=None,
            include_trash=False,
        )

        total = len(event_rows)
        _call_progress(
            progress_cb,
            phase="maintenance_event_extract",
            total=total,
            message=f"Loaded {total} unprocessed event message(s)",
        )

        inserted = 0
        updated = 0
        failed = 0
        processed = 0

        for r in event_rows:
            processed += 1
            try:
                mid = int(r["message_id"])
                gid = str(r["gmail_message_id"])
                subj = r.get("subject")
                from_domain = r.get("from_domain")
                internal_date = r.get("internal_date")
                internal_iso = internal_date.isoformat() if internal_date is not None else None

                body = get_message_body_text(
                    service,
                    message_id=gid,
                    user_id=settings.gmail_user_id,
                    max_chars=30_000,
                )

                extracted = extract_event_from_email(
                    ollama_host=settings.ollama_host,
                    ollama_model=settings.ollama_model,
                    subject=str(subj) if subj is not None else None,
                    from_domain=str(from_domain) if from_domain is not None else None,
                    internal_date_iso=internal_iso,
                    body=body,
                )

                if extracted.event_name or extracted.event_date or extracted.start_time:
                    status = "succeeded"
                else:
                    status = "no_event"

                was_insert = upsert_message_event_metadata(
                    engine=engine,
                    message_id=mid,
                    status=status,
                    error=None,
                    event_name=extracted.event_name,
                    event_type=extracted.event_type,
                    event_date=extracted.event_date,
                    start_time=extracted.start_time,
                    end_time=extracted.end_time,
                    timezone=extracted.timezone,
                    end_time_inferred=bool(extracted.end_time_inferred),
                    confidence=extracted.confidence,
                    model=extracted.model,
                    prompt_version=extracted.prompt_version,
                    raw_json=extracted.raw_json,
                    extracted_at=_now_utc(),
                )

                if was_insert:
                    inserted += 1
                else:
                    updated += 1

                if processed % 25 == 0 or processed == total:
                    _call_progress(
                        progress_cb,
                        phase="maintenance_event_extract",
                        processed=processed,
                        inserted=inserted,
                        skipped_existing=updated,
                        failed=failed,
                        message=(
                            f"Event extraction: {processed}/{total} "
                            f"(ins {inserted}, upd {updated}, fail {failed})"
                        ),
                    )
            except Exception as e:  # noqa: BLE001
                failed += 1
                try:
                    mid = int(r["message_id"])
                    upsert_message_event_metadata(
                        engine=engine,
                        message_id=mid,
                        status="failed",
                        error=str(e),
                        event_name=None,
                        event_type=None,
                        event_date=None,
                        start_time=None,
                        end_time=None,
                        timezone=None,
                        end_time_inferred=False,
                        confidence=None,
                        model=settings.ollama_model,
                        prompt_version=EVENT_PROMPT_VERSION,
                        raw_json=None,
                        extracted_at=_now_utc(),
                    )
                except Exception:
                    pass

                _call_progress(
                    progress_cb,
                    phase="maintenance_event_extract",
                    processed=processed,
                    inserted=inserted,
                    skipped_existing=updated,
                    failed=failed,
                    message=(
                        f"Event extraction: {processed}/{total} "
                        f"(ins {inserted}, upd {updated}, fail {failed})"
                    ),
                )

        _call_progress(
            progress_cb,
            phase="maintenance_event_extract",
            processed=processed,
            inserted=inserted,
            skipped_existing=updated,
            failed=failed,
            message=(
                f"Event extraction done: processed {processed}, "
                f"inserted {inserted}, updated {updated}"
            ),
        )

    if not settings.ollama_host:
        _call_progress(
            progress_cb,
            phase="maintenance_payment_extract",
            message="Skipping payment extraction: Ollama not configured",
        )
    else:
        rows_financial = list_unprocessed_messages_in_category_any_subcategory(
            engine=engine,
            category="Financial",
            limit=None,
            include_trash=False,
        )
        rows_recent = list_unprocessed_messages_received_since(
            engine=engine,
            received_since=cutoff,
            limit=None,
            include_trash=False,
        )

        seen: set[int] = set()
        rows: list[dict[str, Any]] = []
        for r in [*rows_financial, *rows_recent]:
            mid = int(r["message_id"])
            if mid in seen:
                continue
            seen.add(mid)
            rows.append(r)

        total = len(rows)
        _call_progress(
            progress_cb,
            phase="maintenance_payment_extract",
            total=total,
            message=f"Loaded {total} unprocessed payment message(s)",
        )

        inserted = 0
        updated = 0
        failed = 0
        processed = 0

        for r in rows:
            processed += 1
            try:
                mid = int(r["message_id"])
                gid = str(r["gmail_message_id"])
                subj = r.get("subject")
                from_domain = r.get("from_domain")
                internal_date = r.get("internal_date")
                internal_iso = internal_date.isoformat() if internal_date is not None else None

                body = get_message_body_text(
                    service,
                    message_id=gid,
                    user_id=settings.gmail_user_id,
                    max_chars=30_000,
                )

                extracted = extract_payment_from_email(
                    ollama_host=settings.ollama_host,
                    ollama_model=settings.ollama_model,
                    subject=str(subj) if subj is not None else None,
                    from_domain=str(from_domain) if from_domain is not None else None,
                    internal_date_iso=internal_iso,
                    body=body,
                )

                if extracted.cost_amount or extracted.vendor_name or extracted.item_name:
                    status = "succeeded"
                else:
                    status = "no_payment"

                was_insert = upsert_message_payment_metadata(
                    engine=engine,
                    message_id=mid,
                    status=status,
                    error=None,
                    item_name=extracted.item_name,
                    vendor_name=extracted.vendor_name,
                    item_category=extracted.item_category,
                    cost_amount=extracted.cost_amount,
                    cost_currency=extracted.cost_currency,
                    is_recurring=extracted.is_recurring,
                    frequency=extracted.frequency,
                    payment_date=extracted.payment_date,
                    payment_fingerprint=extracted.payment_fingerprint,
                    confidence=extracted.confidence,
                    model=extracted.model,
                    prompt_version=extracted.prompt_version,
                    raw_json=extracted.raw_json,
                    extracted_at=_now_utc(),
                )

                if was_insert:
                    inserted += 1
                else:
                    updated += 1

                if processed % 25 == 0 or processed == total:
                    _call_progress(
                        progress_cb,
                        phase="maintenance_payment_extract",
                        processed=processed,
                        inserted=inserted,
                        skipped_existing=updated,
                        failed=failed,
                        message=(
                            f"Payment extraction: {processed}/{total} "
                            f"(ins {inserted}, upd {updated}, fail {failed})"
                        ),
                    )
            except Exception as e:  # noqa: BLE001
                failed += 1
                try:
                    mid = int(r["message_id"])
                    upsert_message_payment_metadata(
                        engine=engine,
                        message_id=mid,
                        status="failed",
                        error=str(e),
                        item_name=None,
                        vendor_name=None,
                        item_category=None,
                        cost_amount=None,
                        cost_currency=None,
                        is_recurring=None,
                        frequency=None,
                        payment_date=None,
                        payment_fingerprint=None,
                        confidence=None,
                        model=settings.ollama_model,
                        prompt_version=PAYMENT_PROMPT_VERSION,
                        raw_json=None,
                        extracted_at=_now_utc(),
                    )
                except Exception:
                    pass

                _call_progress(
                    progress_cb,
                    phase="maintenance_payment_extract",
                    processed=processed,
                    inserted=inserted,
                    skipped_existing=updated,
                    failed=failed,
                    message=(
                        f"Payment extraction: {processed}/{total} "
                        f"(ins {inserted}, upd {updated}, fail {failed})"
                    ),
                )

        _call_progress(
            progress_cb,
            phase="maintenance_payment_extract",
            processed=processed,
            inserted=inserted,
            skipped_existing=updated,
            failed=failed,
            message=(
                f"Payment extraction done: processed {processed}, "
                f"inserted {inserted}, updated {updated}"
            ),
        )
