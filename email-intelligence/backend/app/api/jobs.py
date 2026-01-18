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
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import APIRouter

from app.api.models import CurrentJobResponse, JobCounters, JobProgress, JobStatusResponse
from app.clustering.pipeline import cluster_and_label
from app.ingestion.metadata_ingestion import ingest_metadata
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


def _now() -> datetime:
    return datetime.now(timezone.utc)


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
        job.updated_at = _now()


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
        # Keep it simple for now; UI should handle this as an error.
        raise KeyError(f"Unknown job_id: {job_id}")
    return _as_response(job)


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
