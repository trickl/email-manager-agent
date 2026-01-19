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
from app.repository.email_query_repository import count_total, count_unlabelled
from app.repository.pipeline_kv_repository import clear_checkpoint_internal_date
from app.repository.policy_repository import get_policy
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
            _set_job(job_id, state="succeeded", message="Done")
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


@router.post("/policies/{policy_id}/run")
def start_run_policy(policy_id: str, limit: int | None = None):
    """Run a deterministic policy evaluation as a background job.

    Stage 2 scope:
    - updates Postgres lifecycle state only (no Gmail mutations)
    - logs intended actions to the audit table
    """

    job_id = _make_job_id("policy-run")
    job = _Job(
        job_id=job_id,
        type="policy_run",
        state="queued",
        phase="policy_evaluation",
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
        from datetime import timedelta

        from app.db.postgres import engine
        from app.policy.engine import select_matching_message_ids
        from app.repository import trash_repository

        rec = get_policy(engine, policy_id)
        if rec is None:
            raise ValueError(f"Unknown policy_id: {policy_id}")

        # Select matching messages.
        ids = select_matching_message_ids(engine, definition=rec.definition, limit=limit)
        _set_job(job_id, total=len(ids), message=f"Selected {len(ids)} candidate messages")

        # Apply action (bulk) + audit.
        retention_days = int(rec.definition.action.retention_days)
        trashed_at = _now()
        expiry_at = trashed_at + timedelta(days=retention_days)

        updated = trash_repository.mark_messages_trashed(
            engine,
            gmail_message_ids=ids,
            policy_id=policy_id,
            retention_days=retention_days,
            trashed_at=trashed_at,
        )

        processed = 0
        for mid in ids:
            processed += 1
            # Best-effort; we don't fetch full message snapshot yet.
            trash_repository.log_action(
                engine,
                action_type="trash",
                gmail_message_id=mid,
                policy_id=policy_id,
                from_state="ACTIVE",
                to_state="TRASHED",
                trashed_at=trashed_at,
                expiry_at=expiry_at,
            )
            if processed % 50 == 0:
                _set_job(job_id, processed=processed)

        _set_job(job_id, processed=len(ids), inserted=updated)

    _run_in_thread(job_id, task)
    return {"job_id": job_id}


@router.post("/retention/expire")
def start_retention_expire(limit: int = 500):
    """Expire TRASHED messages whose retention period has ended.

    Stage 1 scope:
    - transitions Postgres lifecycle_state: TRASHED -> EXPIRED
    - does not hard-delete in Gmail/provider
    """

    limit = max(1, min(int(limit), 5000))

    job_id = _make_job_id("retention-expire")
    job = _Job(
        job_id=job_id,
        type="retention_expire",
        state="queued",
        phase="retention_expire",
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
        from app.repository import trash_repository

        candidates = trash_repository.list_expired_candidates(engine, limit=limit)
        ids = [c["gmail_message_id"] for c in candidates]

        _set_job(job_id, total=len(ids), message=f"Found {len(ids)} expired candidates")

        expired = trash_repository.mark_messages_expired(engine, gmail_message_ids=ids)

        processed = 0
        for c in candidates:
            processed += 1
            trash_repository.log_action(
                engine,
                action_type="expire",
                gmail_message_id=c.get("gmail_message_id"),
                policy_id=c.get("policy_id"),
                from_domain=c.get("from_domain"),
                subject=c.get("subject"),
                internal_date=c.get("internal_date"),
                from_state="TRASHED",
                to_state="EXPIRED",
                trashed_at=c.get("trashed_at"),
                expiry_at=c.get("expiry_at"),
            )
            if processed % 50 == 0:
                _set_job(job_id, processed=processed)

        _set_job(job_id, processed=len(ids), inserted=expired)

    _run_in_thread(job_id, task)
    return {"job_id": job_id}
