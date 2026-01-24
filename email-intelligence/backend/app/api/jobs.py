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
from datetime import datetime, timedelta, timezone
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
    error_samples: list[str]
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


def _add_job_error(job_id: str, sample: str, *, limit: int = 20) -> None:
    """Attach a small number of error samples to a job.

    This is meant for UI/debug visibility ("why did it fail?") without requiring
    access to server logs.
    """

    if not sample:
        return
    with _lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job.error_samples.append(sample)
        if limit > 0 and len(job.error_samples) > int(limit):
            job.error_samples = job.error_samples[-int(limit) :]
        job.updated_at = _now()

        # Push best-effort update (non-fatal).
        try:
            status = _as_response(job)
            payload = json.dumps(jsonable_encoder(status))
            _broadcast(job_id, payload)
        except Exception:
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
        error_samples=list(job.error_samples) if job.error_samples else None,
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


@router.get("/recent", response_model=list[JobStatusResponse])
def recent_jobs(limit: int = 25) -> list[JobStatusResponse]:
    """Return a small list of recent jobs (including failed ones).

    The in-memory job runner only exposes the active job via /current. For debugging
    and UX, it's useful to see the latest jobs without requiring the UI to persist
    job ids.
    """

    limit = max(1, min(int(limit), 200))
    with _lock:
        jobs = list(_jobs.values())
    jobs.sort(key=lambda j: j.started_at, reverse=True)
    return [_as_response(j) for j in jobs[:limit]]


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


def _gmail_count_messages(service, *, user_id: str, q: str | None, page_size: int = 500) -> int | None:
    """Count matching Gmail messages by paging users.messages.list.

    Gmail's resultSizeEstimate can be quite inaccurate for some queries. For long-running
    destructive-ish jobs we prefer an exact pre-count so progress indicators remain sane.
    """

    if page_size < 1:
        page_size = 1
    if page_size > 500:
        page_size = 500

    try:
        total = 0
        page_token: str | None = None
        while True:
            resp = (
                service.users()
                .messages()
                .list(
                    userId=user_id,
                    maxResults=int(page_size),
                    includeSpamTrash=True,
                    q=q,
                    pageToken=page_token,
                )
                .execute()
            )

            msgs = resp.get("messages", []) or []
            total += len(msgs)
            page_token = resp.get("nextPageToken")
            if not page_token:
                return int(total)
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
        error_samples=[],
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
        error_samples=[],
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
        error_samples=[],
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


@router.post("/label/auto")
def start_label_auto(threshold: int = 200):
    """Automatically label unlabelled emails using a simple heuristic.

    Heuristic:
      - if unlabelled < threshold: label each email individually (incremental-style)
      - else: cluster and label in bulk

    This is a long-running job and reports progress via /api/jobs polling + SSE.
    """

    if threshold < 1 or threshold > 50_000:
        raise HTTPException(status_code=400, detail="threshold must be between 1 and 50000")

    settings = Settings()
    job_id = _make_job_id("label-auto")
    job = _Job(
        job_id=job_id,
        type="label_auto",
        state="queued",
        phase="label_auto",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        error_samples=[],
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.db.postgres import engine
        from app.gmail.client import GMAIL_SCOPE_READONLY, get_gmail_service_from_files

        ensure_collection()

        total = count_unlabelled(engine)
        _set_job(job_id, total=int(total))

        if total <= 0:
            _set_job(job_id, phase="label_auto", processed=0, inserted=0, failed=0, message="No unlabelled emails")
            return

        # We only need read access for body sampling; do not request gmail.modify here.
        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            scopes=[GMAIL_SCOPE_READONLY],
            auth_mode=settings.gmail_auth_mode,
            # Background jobs must not block waiting for OAuth consent.
            allow_interactive=False,
        )

        if int(total) < int(threshold):
            _set_job(
                job_id,
                phase="incremental_label",
                message=f"Auto: {total} < {threshold} → labeling individually",
            )

            def hook(
                *,
                emails_processed: int,
                emails_labeled: int,
                emails_failed: int,
                message: str | None,
            ):
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
                max_emails=None,
                progress_hook=hook,
            )
            return

        _set_job(
            job_id,
            phase="cluster_label",
            message=f"Auto: {total} ≥ {threshold} → clustering + bulk labeling",
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
        error_samples=[],
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


@router.post("/gmail/push/outbox")
def start_gmail_push_outbox(batch_size: int = 250):
    """Push taxonomy labels to Gmail for messages currently in label_push_outbox.

    This is the background-job equivalent of /api/gmail-sync/messages/push-incremental.
    It drains the outbox table in batches and records per-row errors.
    """

    if batch_size < 1 or batch_size > 2000:
        raise HTTPException(status_code=400, detail="batch_size must be between 1 and 2000")

    settings = Settings()
    job_id = _make_job_id("gmail-push-outbox")
    job = _Job(
        job_id=job_id,
        type="gmail_push_outbox",
        state="queued",
        phase="gmail_push_outbox",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        error_samples=[],
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from sqlalchemy import text

        from app.db.postgres import engine
        from app.gmail.client import GMAIL_SCOPE_MODIFY, get_gmail_service_from_files, modify_message_labels
        from googleapiclient.errors import HttpError

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            scopes=[GMAIL_SCOPE_MODIFY],
            auth_mode=settings.gmail_auth_mode,
            # Background jobs must not block waiting for OAuth consent.
            allow_interactive=False,
        )

        with engine.begin() as conn:
            total = conn.execute(
                text("SELECT COUNT(*) FROM label_push_outbox WHERE processed_at IS NULL")
            ).scalar()

        if total is not None:
            _set_job(
                job_id,
                total=int(total),
                message=f"Starting outbox push for ~{int(total)} message(s)",
            )

        processed = 0
        succeeded = 0
        failed = 0
        batch_num = 0

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
                except Exception as e:
                    failed += 1
                    with engine.begin() as conn:
                        conn.execute(q_mark_err, {"id": int(outbox_id), "error": str(e)[:5000]})

                if processed % 50 == 0:
                    _set_job(
                        job_id,
                        phase="gmail_push_outbox",
                        processed=processed,
                        inserted=succeeded,
                        failed=failed,
                        message=(
                            f"Pushing outbox… batch {batch_num} (ok {succeeded}, failed {failed})"
                        ),
                    )

            _set_job(
                job_id,
                phase="gmail_push_outbox",
                processed=processed,
                inserted=succeeded,
                failed=failed,
                message=f"Completed batch {batch_num} (ok {succeeded}, failed {failed})",
            )

            time.sleep(0.05)

        _set_job(
            job_id,
            phase="gmail_push_outbox",
            processed=processed,
            inserted=succeeded,
            failed=failed,
            message=f"Finished outbox push: processed {processed}, ok {succeeded}, failed {failed}",
        )

        if failed > 0:
            raise RuntimeError(
                f"Outbox push completed with {failed} failures. "
                "Re-run to retry failed rows; completed rows have been marked processed."
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
        error_samples=[],
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


@router.post("/gmail/archive/trash")
def start_gmail_archive_trash(
    batch_size: int = 250,
    dry_run: bool = False,
    remove_archive_label: bool = False,
):
    """Move all messages with the Archive marker label to Gmail Trash.

    This is intended for the workflow:
      1) Takeout/export your archived mail
      2) Move archived mail to Trash (this job)
      3) Let Gmail auto-expire Trash after ~30 days (or empty Trash manually)

    Notes:
      - Uses the Gmail API "trash" operation (move-to-trash), rather than just adding
        the TRASH system label.
      - By default we do NOT remove other labels (not required for Trash semantics).
      - Optionally remove the archive marker label as well.

    Args:
        batch_size: Max number of messages to fetch and process per iteration. Must be between 1 and 500.
        dry_run: If true, do not mutate Gmail.
        remove_archive_label: If true, remove the archive marker label after trashing.
    """

    if batch_size < 1 or batch_size > 500:
        raise HTTPException(status_code=400, detail="batch_size must be between 1 and 500")

    settings = Settings()
    job_id = _make_job_id("gmail-archive-trash")
    job = _Job(
        job_id=job_id,
        type="gmail_archive_trash",
        state="queued",
        phase="gmail_archive_trash",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        error_samples=[],
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from googleapiclient.errors import HttpError

        from app.gmail.client import (
            GMAIL_SCOPE_MODIFY,
            get_gmail_service_from_files,
            list_labels,
            modify_message_labels,
            move_message_to_trash,
        )
        from app.repository.taxonomy_admin_repository import GMAIL_ARCHIVED_LABEL_NAME

        # Prefer the configured marker label name, but accept fallbacks.
        candidate_label_names = [
            GMAIL_ARCHIVED_LABEL_NAME,
            # Common variant seen in UIs / Takeout labeling.
            "Email archive",
            # User preference / future intent (note: Gmail may reject creating these names,
            # but we can still act on them if they already exist).
            "Archive",
            "Archived",
            # Prior fallbacks used elsewhere.
            "Email-Archive",
            "Archive (marker)",
        ]

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            scopes=[GMAIL_SCOPE_MODIFY],
            auth_mode=settings.gmail_auth_mode,
            # Background jobs must not block waiting for OAuth consent.
            allow_interactive=False,
        )

        labels = list_labels(service, user_id=settings.gmail_user_id)
        name_to_id: dict[str, str] = {}
        for l in labels:
            lid = l.get("id")
            nm = l.get("name")
            if lid and nm:
                name_to_id[str(nm)] = str(lid)

        # Case-insensitive matching helps when users end up with both
        # "Email Archive" and "Email archive".
        candidate_cf = {str(nm).casefold() for nm in candidate_label_names}
        matched_names: list[str] = []
        for nm in name_to_id.keys():
            if str(nm).casefold() in candidate_cf:
                matched_names.append(str(nm))

        archive_label_name: str | None = None
        archive_label_id: str | None = None

        if not matched_names:
            # Keep the error short but actionable.
            available = sorted(name_to_id.keys())
            hint = ", ".join(available[:25])
            more = "" if len(available) <= 25 else f" (+{len(available) - 25} more)"
            raise RuntimeError(
                "Could not find an archive label to trash. "
                f"Tried: {candidate_label_names}. "
                f"Available labels: {hint}{more}"
            )

        # If multiple variants exist (case differences), pick the one with the largest
        # current non-trash membership.
        best_name: str | None = None
        best_total: int | None = None
        for nm in matched_names:
            q_try = f'-in:trash label:"{nm}"'
            n = _gmail_count_messages(service, user_id=settings.gmail_user_id, q=q_try)
            if n is None:
                continue
            if best_total is None or n > best_total:
                best_total = n
                best_name = nm

        archive_label_name = best_name or matched_names[0]
        archive_label_id = str(name_to_id.get(archive_label_name)) if archive_label_name else None

        if not archive_label_name:
            raise RuntimeError("Could not resolve archive label name")

        # Only operate on non-trashed messages to avoid re-processing.
        q = f'-in:trash label:"{archive_label_name}"'

        # Prefer an exact pre-count so progress remains meaningful.
        _set_job(job_id, message=f"Counting messages with label '{archive_label_name}'…")
        total = _gmail_count_messages(service, user_id=settings.gmail_user_id, q=q)
        if total is None:
            # Fall back to Gmail's estimate if counting fails for any reason.
            total = _gmail_total_estimate(service, user_id=settings.gmail_user_id, q=q)
        if total is not None:
            _set_job(job_id, total=int(total), message=f"Found ~{int(total)} message(s) to move to Trash")

        processed = 0
        succeeded = 0
        failed = 0
        batch_num = 0

        def http_error_summary(err: HttpError) -> str:
            """Make a compact, UI-safe description of a Gmail HttpError."""

            status = getattr(getattr(err, "resp", None), "status", None)
            status_str = str(status) if status is not None else "?"

            # Try to pull structured details from the error payload.
            reason: str | None = None
            try:
                raw = getattr(err, "content", None)
                if isinstance(raw, (bytes, bytearray)):
                    payload = json.loads(raw.decode("utf-8", errors="replace"))
                elif isinstance(raw, str):
                    payload = json.loads(raw)
                else:
                    payload = None

                if isinstance(payload, dict):
                    e = payload.get("error")
                    if isinstance(e, dict):
                        # Prefer the human message; include the first machine reason if present.
                        msg = e.get("message")
                        errors = e.get("errors")
                        if isinstance(errors, list) and errors:
                            first = errors[0]
                            if isinstance(first, dict) and first.get("reason"):
                                reason = f"{first.get('reason')}: {msg}" if msg else str(first.get("reason"))
                        if reason is None and msg:
                            reason = str(msg)
            except Exception:
                reason = None

            if reason:
                return f"HttpError {status_str}: {reason}"
            return f"HttpError {status_str}"

        def fetch_batch_ids() -> list[str]:
            resp = (
                service.users()
                .messages()
                .list(
                    userId=settings.gmail_user_id,
                    maxResults=int(batch_size),
                    includeSpamTrash=True,
                    q=q,
                )
                .execute()
            )
            msgs = resp.get("messages", []) or []
            out: list[str] = []
            for m in msgs:
                mid = m.get("id")
                if mid:
                    out.append(str(mid))
            return out

        _set_job(
            job_id,
            message=(
                f"Starting: moving messages with label '{archive_label_name}' to Trash"
                + (" (dry run)" if dry_run else "")
            ),
        )

        while True:
            batch_ids = fetch_batch_ids()
            if not batch_ids:
                break

            batch_num += 1
            for mid in batch_ids:
                processed += 1
                try:
                    if not dry_run:
                        move_message_to_trash(service, message_id=str(mid), user_id=settings.gmail_user_id)
                        if remove_archive_label:
                            if not archive_label_id:
                                raise RuntimeError("archive label id not available")
                            modify_message_labels(
                                service,
                                message_id=str(mid),
                                add_label_ids=None,
                                remove_label_ids=[str(archive_label_id)],
                                user_id=settings.gmail_user_id,
                            )
                    succeeded += 1
                except HttpError as he:
                    status = getattr(getattr(he, "resp", None), "status", None)
                    _add_job_error(job_id, f"{mid}: {http_error_summary(he)}")
                    if (not dry_run) and status in {429, 500, 502, 503, 504}:
                        # Simple best-effort retry.
                        try:
                            time.sleep(0.25)
                            move_message_to_trash(service, message_id=str(mid), user_id=settings.gmail_user_id)
                            if remove_archive_label:
                                if not archive_label_id:
                                    raise RuntimeError("archive label id not available")
                                modify_message_labels(
                                    service,
                                    message_id=str(mid),
                                    add_label_ids=None,
                                    remove_label_ids=[str(archive_label_id)],
                                    user_id=settings.gmail_user_id,
                                )
                            succeeded += 1
                            continue
                        except Exception as retry_exc:  # noqa: BLE001
                            _add_job_error(job_id, f"{mid}: retry_failed: {type(retry_exc).__name__}: {retry_exc}")
                            failed += 1
                            continue

                    failed += 1
                except Exception as exc:  # noqa: BLE001
                    _add_job_error(job_id, f"{mid}: {type(exc).__name__}: {exc}")
                    failed += 1

                if processed % 50 == 0:
                    _set_job(
                        job_id,
                        phase="gmail_archive_trash",
                        processed=processed,
                        inserted=succeeded,
                        failed=failed,
                        message=(
                            f"Moving to Trash… batch {batch_num} "
                            f"(ok {succeeded}, failed {failed})"
                        ),
                    )

            _set_job(
                job_id,
                phase="gmail_archive_trash",
                processed=processed,
                inserted=succeeded,
                failed=failed,
                message=f"Completed batch {batch_num} (ok {succeeded}, failed {failed})",
            )
            time.sleep(0.05)

        _set_job(
            job_id,
            phase="gmail_archive_trash",
            processed=processed,
            inserted=succeeded,
            failed=failed,
            message=f"Finished: processed {processed}, ok {succeeded}, failed {failed}",
        )

        # Do not mark the job as failed purely because of partial per-message failures.
        # For large batches it's common to see a small number of transient 4xx/5xx errors.
        if failed > 0:
            _set_job(
                job_id,
                message=(
                    f"Finished with {failed} failure(s). "
                    "Re-run to retry; already-trashed messages will be skipped. "
                    "See error_samples for details."
                ),
            )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/gmail/trash/sync")
def start_gmail_trash_sync(
    batch_size: int = 500,
    q: str = "in:trash",
):
    """Sync Gmail Trash membership into the DB.

    The dashboard reads counts from Postgres. If emails are moved to Trash in Gmail
    *after* they've been ingested, the DB's label_ids will be stale until a full
    re-ingest (which we want to avoid).

    This job lists message ids matching a Gmail query (default: in:trash) and updates
    the corresponding email_message rows to include the TRASH label id.

    Args:
        batch_size: Gmail list page size (1..500).
        q: Gmail search query to sync. Defaults to "in:trash".
    """

    if batch_size < 1 or batch_size > 500:
        raise HTTPException(status_code=400, detail="batch_size must be between 1 and 500")

    settings = Settings()
    job_id = _make_job_id("gmail-trash-sync")
    job = _Job(
        job_id=job_id,
        type="gmail_trash_sync",
        state="queued",
        phase="gmail_trash_sync",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        error_samples=[],
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from sqlalchemy import text

        from app.db.postgres import engine
        from app.gmail.client import GMAIL_SCOPE_MODIFY, get_gmail_service_from_files

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            # Use gmail.modify so we can reuse an existing token.json minted with modify.
            # In practice this also covers read-only access, and avoids refresh issues
            # when the token was consented with modify only.
            scopes=[GMAIL_SCOPE_MODIFY],
            auth_mode=settings.gmail_auth_mode,
            # Background jobs must not block waiting for OAuth consent.
            allow_interactive=False,
        )

        _set_job(job_id, message=f"Counting Gmail messages matching query: {q!r}…")
        total = _gmail_count_messages(service, user_id=settings.gmail_user_id, q=q)
        if total is None:
            total = _gmail_total_estimate(service, user_id=settings.gmail_user_id, q=q)
        if total is not None:
            _set_job(job_id, total=int(total), message=f"Found ~{int(total)} message(s) to sync")

        update_sql = text(
            """
            UPDATE email_message
            SET label_ids = ARRAY(
                SELECT DISTINCT unnest(COALESCE(label_ids, ARRAY[]::text[]) || ARRAY['TRASH'])
            )
            WHERE gmail_message_id = ANY(:gmail_ids)
            """
        )

        processed = 0
        updated = 0
        failed = 0
        page_token: str | None = None
        batch_num = 0

        while True:
            resp = (
                service.users()
                .messages()
                .list(
                    userId=settings.gmail_user_id,
                    maxResults=int(batch_size),
                    includeSpamTrash=True,
                    q=q,
                    pageToken=page_token,
                )
                .execute()
            )

            msgs = resp.get("messages", []) or []
            ids = [str(m.get("id")) for m in msgs if m.get("id")]

            if ids:
                batch_num += 1
                processed += len(ids)
                try:
                    with engine.begin() as conn:
                        rc = conn.execute(update_sql, {"gmail_ids": ids}).rowcount
                    updated += int(rc or 0)
                except Exception as exc:  # noqa: BLE001
                    failed += len(ids)
                    _add_job_error(job_id, f"batch {batch_num}: {type(exc).__name__}: {exc}")

                # Reuse counters fields to communicate useful info:
                # - inserted: rows updated in DB
                # - skipped_existing: scanned ids not present in DB
                skipped = max(0, processed - updated)
                _set_job(
                    job_id,
                    phase="gmail_trash_sync",
                    processed=processed,
                    inserted=updated,
                    skipped_existing=skipped,
                    failed=failed,
                    message=(
                        f"Syncing Trash labels… batch {batch_num} "
                        f"(scanned {processed}, updated {updated}, missing {skipped}, failed {failed})"
                    ),
                )

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

            time.sleep(0.05)

        skipped = max(0, processed - updated)
        _set_job(
            job_id,
            phase="gmail_trash_sync",
            processed=processed,
            inserted=updated,
            skipped_existing=skipped,
            failed=failed,
            message=(
                f"Finished Trash sync: scanned {processed}, updated {updated}, missing {skipped}, failed {failed}"
            ),
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
        error_samples=[],
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


@router.post("/events/extract/financial-tickets-bookings")
def start_event_extract_financial_tickets_bookings(limit: int = 250):
    """Extract event metadata for emails in Financial / Tickets & Bookings.

    This job:
    - selects messages from Postgres by taxonomy assignment
    - fetches each message body from Gmail (best-effort)
    - calls the local LLM (Ollama) to extract event name/date/start/end
    - stores structured metadata in Postgres attached to the email_message row

    Notes:
    - End times may be inferred (and marked) when not provided explicitly.
    - This is safe/non-destructive: it does not modify Gmail.
    """

    settings = Settings()

    limit = max(1, min(int(limit), 5000))

    job_id = _make_job_id("event-extract")
    job = _Job(
        job_id=job_id,
        type="event_extract",
        state="queued",
        phase="event_extract",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        error_samples=[],
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.analysis.events.extractor import extract_event_from_email
        from app.analysis.events.prompt import PROMPT_VERSION
        from app.db.postgres import engine
        from app.gmail.client import get_gmail_service_from_files, get_message_body_text
        from app.repository.event_metadata_repository import (
            list_messages_in_category,
            upsert_message_event_metadata,
        )

        if not settings.ollama_host:
            raise RuntimeError(
                "Ollama is not configured. Set EMAIL_INTEL_OLLAMA_HOST (e.g. http://localhost:11434)."
            )

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            auth_mode=settings.gmail_auth_mode,
            allow_interactive=settings.gmail_allow_interactive,
        )

        rows = list_messages_in_category(
            engine=engine,
            category="Financial",
            subcategory="Tickets & Bookings",
            limit=limit,
        )

        total = len(rows)
        _set_job(job_id, total=total, phase="event_extract", message=f"Loaded {total} messages")

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

                extracted = extract_event_from_email(
                    ollama_host=settings.ollama_host,
                    ollama_model=settings.ollama_model,
                    subject=str(subj) if subj is not None else None,
                    from_domain=str(from_domain) if from_domain is not None else None,
                    internal_date_iso=internal_iso,
                    body=body,
                )

                # Decide a simple status.
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
                    extracted_at=_now(),
                )

                if was_insert:
                    inserted += 1
                else:
                    updated += 1

                _set_job(
                    job_id,
                    phase="event_extract",
                    processed=processed,
                    inserted=inserted,
                    skipped_existing=updated,
                    failed=failed,
                    message=f"Extracted events: {processed}/{total} (inserted {inserted}, updated {updated}, failed {failed})",
                )
            except Exception as e:  # noqa: BLE001
                failed += 1
                _add_job_error(job_id, f"event_extract_failed message_id={r.get('gmail_message_id')}: {e}")

                # Best-effort: persist the failure row so we have visibility.
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
                        prompt_version=PROMPT_VERSION,
                        raw_json=None,
                        extracted_at=_now(),
                    )
                except Exception:
                    pass

                _set_job(
                    job_id,
                    phase="event_extract",
                    processed=processed,
                    inserted=inserted,
                    skipped_existing=updated,
                    failed=failed,
                    message=f"Extracted events: {processed}/{total} (inserted {inserted}, updated {updated}, failed {failed})",
                )

        _set_job(
            job_id,
            phase="event_extract",
            processed=processed,
            inserted=inserted,
            skipped_existing=updated,
            failed=failed,
            message=f"Done: processed {processed}, inserted {inserted}, updated {updated}, failed {failed}",
        )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/payments/extract/financial-and-recent")
def start_payment_extract_financial_and_recent(days: int = 12):
    """Extract payment metadata for all Financial emails and recent emails.

    This job:
    - extracts payments for all messages tagged with category=Financial (not TRASH)
    - scans the last N days of Gmail, upserts metadata, and extracts payments (not TRASH)

    Notes:
    - This is safe/non-destructive: it does not modify Gmail.
    - De-duplication is handled downstream via payment_fingerprint.
    """

    settings = Settings()

    days = max(1, int(days))

    job_id = _make_job_id("payment-extract")
    job = _Job(
        job_id=job_id,
        type="payment_extract",
        state="queued",
        phase="payment_extract",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        error_samples=[],
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.analysis.payments.extractor import extract_payment_from_email
        from app.analysis.payments.prompt import PROMPT_VERSION
        from app.db.postgres import engine
        from app.gmail.client import (
            get_gmail_service_from_files,
            get_message_body_text,
            get_message_metadata,
            iter_message_ids,
        )
        from app.gmail.mapping import metadata_to_domain
        from app.repository.email_repository import insert_email
        from app.repository.payment_metadata_repository import (
            list_messages_in_category_any_subcategory,
            list_messages_received_since,
            upsert_message_payment_metadata,
        )

        if not settings.ollama_host:
            raise RuntimeError(
                "Ollama is not configured. Set EMAIL_INTEL_OLLAMA_HOST (e.g. http://localhost:11434)."
            )

        service = get_gmail_service_from_files(
            credentials_path=settings.gmail_credentials_path,
            token_path=settings.gmail_token_path,
            auth_mode=settings.gmail_auth_mode,
            allow_interactive=settings.gmail_allow_interactive,
        )

        # Phase 1: Financial category (tier-1) extraction.
        rows_financial = list_messages_in_category_any_subcategory(
            engine=engine,
            category="Financial",
            limit=None,
        )

        # Phase 2: Recent email metadata sync + extraction.
        cutoff = _now() - timedelta(days=days)
        cutoff_q = f"after:{int((cutoff - timedelta(seconds=1)).timestamp())}"

        scanned = 0
        inserted_meta = 0
        failed_meta = 0

        _set_job(
            job_id,
            phase="payment_extract",
            message=f"Syncing recent metadata (last {days} days)",
        )

        for msg_id in iter_message_ids(
            service,
            user_id=settings.gmail_user_id,
            page_size=500,
            q=cutoff_q,
        ):
            scanned += 1
            try:
                meta = get_message_metadata(
                    service, message_id=msg_id, user_id=settings.gmail_user_id
                )
                email = metadata_to_domain(meta)

                if email.internal_date.tzinfo is None:
                    email.internal_date = email.internal_date.replace(tzinfo=timezone.utc)
                if email.internal_date < cutoff:
                    continue

                insert_email(email)
                inserted_meta += 1

                if scanned % 250 == 0:
                    _set_job(
                        job_id,
                        phase="payment_extract",
                        message=(
                            "Syncing recent metadata: "
                            f"scanned={scanned} upserted={inserted_meta} failed={failed_meta}"
                        ),
                    )
            except Exception as e:  # noqa: BLE001
                failed_meta += 1
                _add_job_error(job_id, f"payment_recent_metadata_failed {msg_id}: {e}")

        rows_recent = list_messages_received_since(
            engine=engine,
            received_since=cutoff,
            limit=None,
            include_trash=False,
        )

        total = len(rows_financial) + len(rows_recent)
        _set_job(job_id, total=total, phase="payment_extract", message=f"Loaded {total} messages")

        inserted = 0
        updated = 0
        failed = 0
        processed = 0

        def _process_rows(rows: list[dict[str, object]], label: str) -> None:
            nonlocal inserted, updated, failed, processed
            for r in rows:
                processed += 1
                try:
                    mid = int(r["message_id"])  # type: ignore[index]
                    gid = str(r["gmail_message_id"])  # type: ignore[index]
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
                        extracted_at=_now(),
                    )

                    if was_insert:
                        inserted += 1
                    else:
                        updated += 1

                    if processed % 25 == 0 or processed == total:
                        _set_job(
                            job_id,
                            phase="payment_extract",
                            processed=processed,
                            inserted=inserted,
                            skipped_existing=updated,
                            failed=failed,
                            message=(
                                f"{label}: {processed}/{total} (ins {inserted}, "
                                f"upd {updated}, fail {failed})"
                            ),
                        )
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    _add_job_error(job_id, f"payment_extract_failed {r.get('gmail_message_id')}: {e}")

                    try:
                        mid = int(r["message_id"])  # type: ignore[index]
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
                            prompt_version=PROMPT_VERSION,
                            raw_json=None,
                            extracted_at=_now(),
                        )
                    except Exception:
                        pass

                    _set_job(
                        job_id,
                        phase="payment_extract",
                        processed=processed,
                        inserted=inserted,
                        skipped_existing=updated,
                        failed=failed,
                        message=(
                            f"{label}: {processed}/{total} (ins {inserted}, "
                            f"upd {updated}, fail {failed})"
                        ),
                    )

        _process_rows(rows_financial, "Financial")
        _process_rows(rows_recent, "Recent")

        _set_job(
            job_id,
            phase="payment_extract",
            processed=processed,
            inserted=inserted,
            skipped_existing=updated,
            failed=failed,
            message=f"Done: processed {processed}, inserted {inserted}, updated {updated}, failed {failed}",
        )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/maintenance/run")
def start_maintenance(
    inbox_cleanup_days: int | None = None,
    label_threshold: int | None = None,
    fallback_days: int | None = None,
):
    """Run the full maintenance pipeline incrementally."""

    settings = Settings()

    job_id = _make_job_id("maintenance")
    job = _Job(
        job_id=job_id,
        type="maintenance",
        state="queued",
        phase="maintenance",
        started_at=_now(),
        updated_at=_now(),
        progress_total=None,
        progress_processed=0,
        counters=JobCounters(),
        message="Queued",
        error_samples=[],
        eta_hint=None,
    )
    with _lock:
        _jobs[job_id] = job

    def task():
        from app.db.postgres import engine
        from app.maintenance import run_maintenance

        def progress_cb(
            *,
            phase: str,
            message: str | None = None,
            processed: int | None = None,
            total: int | None = None,
            inserted: int | None = None,
            skipped_existing: int | None = None,
            failed: int | None = None,
        ) -> None:
            _set_job(
                job_id,
                phase=phase,
                message=message,
                processed=processed,
                total=total,
                inserted=inserted,
                skipped_existing=skipped_existing,
                failed=failed,
            )

        run_maintenance(
            engine=engine,
            settings=settings,
            inbox_cleanup_days=inbox_cleanup_days,
            label_threshold=label_threshold,
            fallback_days=fallback_days,
            allow_interactive=False,
            progress_cb=progress_cb,
        )

    _run_in_thread(job_id, task)
    return {"job_id": job_id}



