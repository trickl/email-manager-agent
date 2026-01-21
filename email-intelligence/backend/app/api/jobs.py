"""Job control API.

This provides polling-friendly job endpoints:
- job start endpoints return immediately with job_id
- job status endpoints are cheap and can be polled

Implementation note:
This is an in-memory job runner suitable for Phase 1 UI wiring.
"""

from __future__ import annotations

import threading
import time
import uuid
import queue
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from starlette.responses import StreamingResponse

from app.api.models import CurrentJobResponse, JobCounters, JobProgress, JobStatusResponse
from app.clustering.pipeline import cluster_and_label
from app.ingestion.metadata_ingestion import ingest_metadata
from app.labeling.incremental_pipeline import label_unlabelled_individual
from app.repository.email_query_repository import count_total, count_unlabelled
from app.repository.pipeline_kv_repository import clear_checkpoint_internal_date
from app.settings import Settings
from app.vector.qdrant import ensure_collection

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


@dataclass
class _Job:
    job_id: str
    type: str
    state: str
    phase: str | None
    started_at: datetime
    updated_at: datetime
    progress_total: int | None
    progress_processed: int
    counters: JobCounters
    message: str | None
    eta_hint: str | None


_jobs: dict[str, _Job] = {}
_lock = threading.Lock()

# Simple in-memory pub/sub for pushing job status updates (SSE).
#
# This is intentionally lightweight and process-local:
# - suitable for local/dev and single-process deployments
# - avoids polling overhead in the browser
#
# Note: in multi-worker deployments, clients must reconnect to the same worker
# to receive updates (sticky sessions) OR we should switch to Redis/pubsub.
_subscribers: dict[str, set[queue.Queue[str]]] = {}
_sub_lock = threading.Lock()


def _subscribe(job_id: str) -> queue.Queue[str]:
    q: queue.Queue[str] = queue.Queue(maxsize=25)
    with _sub_lock:
        _subscribers.setdefault(job_id, set()).add(q)
    return q


def _unsubscribe(job_id: str, q: queue.Queue[str]) -> None:
    with _sub_lock:
        subs = _subscribers.get(job_id)
        if not subs:
            return
        subs.discard(q)
        if not subs:
            _subscribers.pop(job_id, None)


def _broadcast(job_id: str, payload: str) -> None:
    # Never block the job runner thread.
    with _sub_lock:
        subs = list(_subscribers.get(job_id, set()))

    for q in subs:
        try:
            q.put_nowait(payload)
        except queue.Full:
            # Drop oldest and retry once.
            try:
                _ = q.get_nowait()
            except queue.Empty:
                pass
            try:
                q.put_nowait(payload)
            except queue.Full:
                # Still full, drop this update.
                pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _format_eta(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    if seconds < 0:
        return None

    s = int(seconds)
    if s < 60:
        return f"~{s}s"
    if s < 3600:
        m = max(1, s // 60)
        return f"~{m}m"
    h = s // 3600
    m = (s % 3600) // 60
    return f"~{h}h {m}m"


def _compute_eta_hint(*, started_at: datetime, processed: int, total: int | None) -> str | None:
    if not total or total <= 0:
        return None
    if processed <= 0:
        return None
    if processed >= total:
        return "~0s"

    elapsed = (_now() - started_at).total_seconds()
    if elapsed <= 0:
        return None

    rate = processed / elapsed
    if rate <= 0:
        return None

    remaining = total - processed
    return _format_eta(remaining / rate)


def _make_job_id(prefix: str) -> str:
    stamp = _now().strftime("%Y%m%d-%H%M%S")
    return f"job-{stamp}-{prefix}-{uuid.uuid4().hex[:6]}"


def _set_job(
    job_id: str,
    *,
    state: str | None = None,
    phase: str | None = None,
    processed: int | None = None,
    total: int | None = None,
    inserted: int | None = None,
    skipped_existing: int | None = None,
    failed: int | None = None,
    message: str | None = None,
):
    with _lock:
        job = _jobs[job_id]
        if state is not None:
            job.state = state
        if phase is not None:
            job.phase = phase
        if processed is not None:
            job.progress_processed = processed
        if total is not None:
            job.progress_total = total
        if inserted is not None:
            job.counters.inserted = inserted
        if skipped_existing is not None:
            job.counters.skipped_existing = skipped_existing
        if failed is not None:
            job.counters.failed = failed
        if message is not None:
            job.message = message

        # Best-effort ETA based on elapsed time and processed/total.
        job.eta_hint = _compute_eta_hint(
            started_at=job.started_at,
            processed=job.progress_processed,
            total=job.progress_total,
        )
        job.updated_at = _now()

        # Push best-effort status update to any SSE subscribers.
        try:
            status = _as_response(job)
            payload = json.dumps(jsonable_encoder(status))
            _broadcast(job_id, payload)
        except Exception:
            # Do not let notification failures affect job execution.
            pass


def _as_response(job: _Job) -> JobStatusResponse:
    percent = None
    if job.progress_total and job.progress_total > 0:
        percent = 100.0 * (job.progress_processed / job.progress_total)

    return JobStatusResponse(
        job_id=job.job_id,
        type=job.type,
        state=job.state,
        phase=job.phase,
        started_at=job.started_at,
        updated_at=job.updated_at,
        progress=JobProgress(total=job.progress_total, processed=job.progress_processed, percent=percent),
        counters=job.counters,
        message=job.message,
        eta_hint=job.eta_hint,
    )


def _active_job() -> _Job | None:
    with _lock:
        for job in _jobs.values():
            if job.state == "running":
                return job
    return None


@router.get("/current", response_model=CurrentJobResponse)
def current_job() -> CurrentJobResponse:
    job = _active_job()
    if not job:
        return CurrentJobResponse(active=None)
    return CurrentJobResponse(active={"job_id": job.job_id, "type": job.type, "state": job.state})


@router.get("/{job_id}/status", response_model=JobStatusResponse)
def job_status(job_id: str) -> JobStatusResponse:
    with _lock:
        job = _jobs.get(job_id)
    if not job:
        # Use a proper HTTP error so clients don't see a 500.
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    return _as_response(job)


@router.get("/{job_id}/events")
def job_events(job_id: str):
    """Stream job status updates via Server-Sent Events (SSE).

    Events:
      - event: job_status
        data: <JobStatusResponse JSON>

    Keepalive comments are sent periodically.
    """

    with _lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")

    q = _subscribe(job_id)

    def gen() -> Iterator[bytes]:
        try:
            # Initial connect comment (helps some proxies establish the stream).
            yield b": connected\n\n"

            # Emit the current snapshot immediately.
            try:
                with _lock:
                    cur = _jobs.get(job_id)
                if cur is not None:
                    payload = json.dumps(jsonable_encoder(_as_response(cur)))
                    yield f"event: job_status\ndata: {payload}\n\n".encode("utf-8")
            except Exception:
                pass

            # Stream subsequent updates.
            while True:
                try:
                    payload = q.get(timeout=15.0)
                    yield f"event: job_status\ndata: {payload}\n\n".encode("utf-8")
                except queue.Empty:
                    # Keepalive
                    yield b": keep-alive\n\n"
        finally:
            _unsubscribe(job_id, q)

    return StreamingResponse(gen(), media_type="text/event-stream")


def _run_in_thread(job_id: str, fn):
    def runner():
        _set_job(job_id, state="running")
        try:
            fn()
            # Preserve any final summary message set by the job task.
            with _lock:
                msg = _jobs.get(job_id).message if job_id in _jobs else None
            _set_job(job_id, state="succeeded", message=msg or "Done")
        except Exception as exc:  # noqa: BLE001
            _set_job(job_id, state="failed", message=str(exc))

    t = threading.Thread(target=runner, daemon=True)
    t.start()


def _gmail_total_estimate(service, *, user_id: str, q: str | None) -> int | None:
    try:
        resp = (
            service.users()
            .messages()
            .list(userId=user_id, maxResults=1, includeSpamTrash=True, q=q)
            .execute()
        )
        est = resp.get("resultSizeEstimate")
        return int(est) if est is not None else None
    except Exception:
        return None


@router.post("/ingest/full")
def start_ingest_full():
    settings = Settings()
    job_id = _make_job_id("ingest-full")
    job = _Job(
        job_id=job_id,
        type="ingest_full",
        state="queued",
        phase="metadata_ingestion",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.db.postgres import engine
        from app.gmail.client import get_gmail_service_from_files

        ensure_collection()
        clear_checkpoint_internal_date(engine)

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
        )

        total = _gmail_total_estimate(service, user_id=settings.gmail_user_id, q=None)
        if total is not None:
            _set_job(job_id, total=total)

        def hook(*, processed: int, skipped: int, failed: int, message: str | None):
            _set_job(
                job_id,
                phase="metadata_ingestion",
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
            progress_hook=hook,
        )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/ingest/refresh")
def start_ingest_refresh():
    settings = Settings()
    job_id = _make_job_id("ingest-refresh")
    job = _Job(
        job_id=job_id,
        type="ingest_refresh",
        state="queued",
        phase="metadata_ingestion",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.db.postgres import engine
        from app.gmail.client import get_gmail_service_from_files

        ensure_collection()

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
        )

        # Estimate using the same checkpoint query (best-effort).
        checkpoint = None
        from app.repository.pipeline_kv_repository import get_checkpoint_internal_date

        checkpoint = get_checkpoint_internal_date(engine)
        q = None
        if checkpoint:
            q = f"after:{int(checkpoint.timestamp())}"

        total = _gmail_total_estimate(service, user_id=settings.gmail_user_id, q=q)
        if total is not None:
            _set_job(job_id, total=total)

        def hook(*, processed: int, skipped: int, failed: int, message: str | None):
            _set_job(
                job_id,
                phase="metadata_ingestion",
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
            progress_hook=hook,
        )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/cluster-label/run")
def start_cluster_label():
    settings = Settings()
    job_id = _make_job_id("cluster-label")
    job = _Job(
        job_id=job_id,
        type="cluster_label",
        state="queued",
        phase="cluster_label",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.db.postgres import engine
        from app.gmail.client import get_gmail_service_from_files

        ensure_collection()

        total = count_unlabelled(engine)
        _set_job(job_id, total=total)

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
        )

        def hook(*, clusters_done: int, emails_labeled: int, message: str | None):
            _set_job(
                job_id,
                phase="cluster_label",
                processed=emails_labeled,
                inserted=emails_labeled,
                message=message,
            )

        cluster_and_label(
            engine=engine,
            service=service,
            user_id=settings.gmail_user_id,
            similarity_threshold=settings.similarity_threshold,
            label_version=settings.label_version,
            ollama_host=settings.ollama_host,
            ollama_model=settings.ollama_model,
            max_clusters=None,
            progress_hook=hook,
        )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/gmail/push/bulk")
def start_gmail_push_bulk(batch_size: int = 200):
    """Push taxonomy label membership to Gmail for all labeled messages.

    This is intentionally implemented as a backend job so it can:
    - run longer than typical HTTP/proxy timeouts
    - report progress (% + ETA) via the existing /api/jobs polling + SSE

    Args:
        batch_size: Number of messages to process per DB batch. Gmail API calls are still
            per-message; batch_size controls DB paging and status update cadence.
    """

    if batch_size < 1 or batch_size > 2000:
        raise HTTPException(status_code=400, detail="batch_size must be between 1 and 2000")

    settings = Settings()
    job_id = _make_job_id("gmail-push-bulk")
    job = _Job(
        job_id=job_id,
        type="gmail_push_bulk",
        state="queued",
        phase="gmail_push",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from sqlalchemy import text

        from app.db.postgres import engine
        from app.gmail.client import GMAIL_SCOPE_MODIFY, get_gmail_service_from_files, modify_message_labels
        from googleapiclient.errors import HttpError

        # Use modify scope: required for users.messages.modify.
        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            scopes=[GMAIL_SCOPE_MODIFY],
            auth_mode=settings.gmail_auth_mode,
            # Background jobs must not block waiting for OAuth consent.
            allow_interactive=False,
        )

        # Total messages eligible to be pushed (best-effort).
        with engine.begin() as conn:
            total = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT em.id
                        FROM email_message em
                        JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
                        JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
                        WHERE tl.is_active = TRUE
                        GROUP BY em.id
                    ) s
                    """
                )
            ).scalar()

        if total is not None:
            _set_job(job_id, total=int(total), message=f"Starting Gmail push for ~{int(total)} messages")

        processed = 0
        succeeded = 0
        failed = 0

        after_id = 0
        batch_num = 0

        q = text(
            """
            SELECT
                em.id AS message_id,
                em.gmail_message_id AS gmail_message_id,
                ARRAY_AGG(DISTINCT tl.gmail_label_id) AS gmail_label_ids
            FROM email_message em
            JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
            JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
            WHERE tl.is_active = TRUE
                            AND em.gmail_message_id IS NOT NULL
              AND em.id > :after_id
            GROUP BY em.id, em.gmail_message_id
            ORDER BY em.id ASC
            LIMIT :limit
            """
        )

        while True:
            with engine.begin() as conn:
                rows = conn.execute(q, {"after_id": int(after_id), "limit": int(batch_size)}).mappings().all()

            if not rows:
                break

            batch_num += 1
            for r in rows:
                mid = int(r["message_id"])
                gmail_message_id = str(r["gmail_message_id"])
                after_id = max(after_id, mid)

                processed += 1
                add_ids = [str(x) for x in (r["gmail_label_ids"] or []) if x is not None and str(x).strip()]

                if not add_ids:
                    failed += 1
                else:
                    try:
                        modify_message_labels(
                            service,
                            message_id=gmail_message_id,
                            add_label_ids=add_ids,
                            remove_label_ids=None,
                            user_id=settings.gmail_user_id,
                        )
                        succeeded += 1
                    except HttpError as he:
                        # Best-effort retry for transient Gmail/API/network errors.
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
                                succeeded += 1
                                continue
                            except Exception:
                                pass
                        failed += 1
                    except Exception:
                        failed += 1

                # Update status occasionally (every 50 messages) to keep UI responsive without
                # overwhelming SSE/polling with updates.
                if processed % 50 == 0:
                    _set_job(
                        job_id,
                        phase="gmail_push",
                        processed=processed,
                        inserted=succeeded,
                        failed=failed,
                        message=(
                            f"Pushing Gmail labels… batch {batch_num}, last_id {after_id} "
                            f"(ok {succeeded}, failed {failed})"
                        ),
                    )

            # Batch boundary update.
            _set_job(
                job_id,
                phase="gmail_push",
                processed=processed,
                inserted=succeeded,
                failed=failed,
                message=(
                    f"Pushing Gmail labels… completed batch {batch_num} (ok {succeeded}, failed {failed})"
                ),
            )

            # Be gentle to the Gmail API.
            time.sleep(0.05)

        _set_job(
            job_id,
            phase="gmail_push",
            processed=processed,
            inserted=succeeded,
            failed=failed,
            message=f"Finished Gmail push: processed {processed}, ok {succeeded}, failed {failed}",
        )

        # If we encountered failures (common during connectivity outages), surface this
        # via job state so the UI doesn't imply everything is applied.
        if failed > 0:
            raise RuntimeError(
                f"Gmail push completed with {failed} failures. "
                "This usually means transient Gmail/API/network errors; re-run the job to resume."
            )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/gmail/archive/push")
def start_gmail_archive_push(batch_size: int = 200, dry_run: bool = False):
    """Apply the Gmail Archive marker label for messages queued by retention planning.

    This job consumes rows from archive_push_outbox (planned via /api/gmail-sync/retention/plan)
    and adds the marker label in Gmail.

    It is implemented as a job because it can take a long time for large inboxes.

    Args:
        batch_size: Number of outbox rows to process per batch.
        dry_run: If true, do not call Gmail or mutate DB processed flags.
    """

    if batch_size < 1 or batch_size > 2000:
        raise HTTPException(status_code=400, detail="batch_size must be between 1 and 2000")

    settings = Settings()
    job_id = _make_job_id("gmail-archive-push")
    job = _Job(
        job_id=job_id,
        type="gmail_archive_push",
        state="queued",
        phase="gmail_archive_push",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.db.postgres import engine
        from app.gmail.client import (
            GMAIL_SCOPE_MODIFY,
            create_label,
            get_gmail_service_from_files,
            label_name_to_id,
            modify_message_labels,
        )
        from app.repository.retention_archive_repository import count_pending_outbox, fetch_pending_batch, mark_outbox_failed, mark_outbox_succeeded
        from app.repository.taxonomy_admin_repository import GMAIL_ARCHIVED_LABEL_NAME
        from googleapiclient.errors import HttpError

        # Marker label name: Gmail rejects bare "Archive"/"Archived".
        # Use our configured marker label name and keep a few fallbacks.
        candidate_label_names = [
            GMAIL_ARCHIVED_LABEL_NAME,
            "Email-Archive",
            "Archive (marker)",
        ]

        # Best-effort total (pending outbox rows).
        total = count_pending_outbox(engine=engine)
        _set_job(job_id, total=int(total), message=f"Starting archive push for {int(total)} message(s)")

        if dry_run:
            # IMPORTANT: In dry-run, do not call Gmail or mutate DB flags.
            # We also cannot loop until "empty" because the outbox will remain pending.
            batch = fetch_pending_batch(engine=engine, limit=int(batch_size))
            processed = len(batch)
            _set_job(
                job_id,
                phase="gmail_archive_push",
                processed=processed,
                inserted=processed,
                failed=0,
                message=(
                    f"Dry run: would apply '{GMAIL_ARCHIVED_LABEL_NAME}' label to {processed} message(s) "
                    "(sample batch)"
                ),
            )
            return

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            scopes=[GMAIL_SCOPE_MODIFY],
            auth_mode=settings.gmail_auth_mode,
            # Background jobs must not block waiting for OAuth consent.
            allow_interactive=False,
        )

        # Ensure marker label exists.
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
                    # Gmail can reject certain reserved names (400 Invalid label name).
                    last_err = he
                    continue
                except Exception as e:
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
                    if not dry_run:
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
                    # Best-effort retry for transient Gmail/API/network errors.
                    status = getattr(getattr(he, "resp", None), "status", None)
                    if (not dry_run) and status in {429, 500, 502, 503, 504}:
                        try:
                            time.sleep(0.25)
                            modify_message_labels(
                                service,
                                message_id=row.gmail_message_id,
                                add_label_ids=[str(archived_label_id)],
                                remove_label_ids=None,
                                user_id=settings.gmail_user_id,
                            )
                            mark_outbox_succeeded(engine=engine, outbox_id=row.id, message_id=row.message_id)
                            succeeded += 1
                            continue
                        except Exception as e:
                            failed += 1
                            if not dry_run:
                                mark_outbox_failed(engine=engine, outbox_id=row.id, error=str(e))
                            continue

                    failed += 1
                    if not dry_run:
                        mark_outbox_failed(engine=engine, outbox_id=row.id, error=str(he))
                except Exception as e:
                    failed += 1
                    if not dry_run:
                        mark_outbox_failed(engine=engine, outbox_id=row.id, error=str(e))

                if processed % 50 == 0:
                    _set_job(
                        job_id,
                        phase="gmail_archive_push",
                        processed=processed,
                        inserted=succeeded,
                        failed=failed,
                        message=(
                            f"Applying '{GMAIL_ARCHIVED_LABEL_NAME}' label… batch {batch_num} (ok {succeeded}, failed {failed})"
                        ),
                    )

            _set_job(
                job_id,
                phase="gmail_archive_push",
                processed=processed,
                inserted=succeeded,
                failed=failed,
                message=f"Completed batch {batch_num} (ok {succeeded}, failed {failed})",
            )
            time.sleep(0.05)

        _set_job(
            job_id,
            phase="gmail_archive_push",
            processed=processed,
            inserted=succeeded,
            failed=failed,
            message=f"Finished archive push: processed {processed}, ok {succeeded}, failed {failed}",
        )

        if failed > 0:
            raise RuntimeError(
                f"Archive push completed with {failed} failures. "
                "Re-run after planning again to retry failed rows."
            )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/incremental/run")
def start_incremental_run(max_messages: int | None = None, max_emails: int | None = None):
    """Run the daily incremental pipeline.

    Steps:
      1) Phase 1: metadata-only ingestion (incremental by checkpoint)
      2) Phase 2: per-email labeling for any remaining unlabelled emails

    This intentionally avoids the bulk-mode clustering optimization.
    """

    settings = Settings()

    if max_messages is not None:
        max_messages = max(1, min(int(max_messages), 5000))
    if max_emails is not None:
        max_emails = max(1, min(int(max_emails), 5000))

    job_id = _make_job_id("incremental")
    job = _Job(
        job_id=job_id,
        type="incremental",
        state="queued",
        phase="metadata_ingestion",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.db.postgres import engine
        from app.gmail.client import get_gmail_service_from_files

        ensure_collection()

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
        )

        # Phase 1: incremental metadata ingest
        def ingest_hook(*, processed: int, skipped: int, failed: int, message: str | None):
            _set_job(
                job_id,
                phase="metadata_ingestion",
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
            max_messages=max_messages,
            progress_hook=ingest_hook,
        )

        # Phase 2: label any remaining unlabelled emails, one-by-one.
        total_unlabelled = count_unlabelled(engine)
        _set_job(job_id, phase="incremental_label", total=total_unlabelled)

        def label_hook(
            *,
            emails_processed: int,
            emails_labeled: int,
            emails_failed: int,
            message: str | None,
        ):
            # Report progress as "emails labeled" because that's the visible outcome.
            _set_job(
                job_id,
                phase="incremental_label",
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
            max_emails=max_emails,
            progress_hook=label_hook,
        )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}



