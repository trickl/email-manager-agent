"""FastAPI application stub (Phase 0).

Goal: a live backend with a health endpoint and real service wiring, but no
business logic or ingestion.
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Phase 0 requirement: test Postgres connection on import.
# Importing this module will fail fast if the DB is unreachable.
from app.db.postgres import engine as _postgres_engine
from app.db.schema import ensure_core_schema
from app.ingestion.metadata_ingestion import ingest_metadata
from app.ingestion.pipeline import ingest_fake_email
from app.repository.email_query_repository import count_clusters
from app.repository.email_query_repository import count_labelled
from app.repository.email_query_repository import count_total
from app.repository.email_query_repository import count_unlabelled
from app.repository.pipeline_kv_repository import KEY_CURRENT_PHASE
from app.repository.pipeline_kv_repository import get_checkpoint_internal_date
from app.repository.pipeline_kv_repository import get_kv
from app.repository.pipeline_kv_repository import set_current_phase
from app.repository.policy_repository import ensure_default_policies
from app.repository.taxonomy_repository import ensure_taxonomy_seeded
from app.settings import Settings
from app.settings import StatusResponse
from app.vector.qdrant import ensure_collection
from app.vector.qdrant import query_similar
from app.vector.vectorizer import vectorize_text
from app.vector.vectorizer import vector_version_tag
from app.clustering.pipeline import cluster_and_label
from app.gmail.client import get_gmail_service_from_files

from app.api.dashboard import router as dashboard_router
from app.api.jobs import router as jobs_router
from app.api.messages import router as messages_router
from app.api.policies import router as policies_router
from app.api.trash import router as trash_router

app = FastAPI(title="Email Intelligence Backend")

# Local/dev CORS: React (Vite) defaults to http://localhost:5173.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"] ,
    allow_headers=["*"],
)

app.include_router(dashboard_router)
app.include_router(jobs_router)
app.include_router(messages_router)
app.include_router(policies_router)
app.include_router(trash_router)


@app.on_event("startup")
def _startup() -> None:
    # Ensure required schema exists even for already-initialized DB volumes.
    ensure_core_schema(_postgres_engine)

    # Stage 2: seed a canonical deterministic policy (idempotent).
    ensure_default_policies(_postgres_engine)

    # Ensure our pre-seeded taxonomy exists before any ingestion starts.
    ensure_taxonomy_seeded(_postgres_engine)

    # Phase 0 requirement: ensure the (empty) collection exists.
    ensure_collection()

    # Default phase on boot.
    set_current_phase(_postgres_engine, "idle")

    # Optional: keep the previous Phase 1 fake seed behind an explicit flag.
    if os.getenv("EMAIL_INTEL_SEED_FAKE_EMAIL", "false").lower() in {"1", "true", "yes"}:
        ingest_fake_email()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/test/vector-search")
def test_vector_search():
    # Use a real embedding so this endpoint actually indicates whether vectors are meaningful.
    vector = vectorize_text("Subject: flight tickets receipt.\nSender domain: example.com.\n")
    results = query_similar(vector, vector_version=vector_version_tag())
    return {"matches": len(results)}


@app.get("/status", response_model=StatusResponse)
def status() -> StatusResponse:
    checkpoint = get_checkpoint_internal_date(_postgres_engine)
    current_phase = get_kv(_postgres_engine, KEY_CURRENT_PHASE)

    total = count_total(_postgres_engine)
    labelled = count_labelled(_postgres_engine)
    unlabelled = count_unlabelled(_postgres_engine)
    clusters = count_clusters(_postgres_engine)

    return StatusResponse(
        current_phase=current_phase,
        total_email_count=total,
        labelled_email_count=labelled,
        unlabelled_email_count=unlabelled,
        cluster_count=clusters,
        estimated_remaining_clusters=unlabelled,
        last_ingested_internal_date=checkpoint.isoformat() if checkpoint else None,
    )


@app.post("/pipeline/ingest-metadata")
def run_ingest_metadata(max_messages: int | None = None):
    settings = Settings()
    service = get_gmail_service_from_files(
        credentials_path=settings.gmail_credentials_path,
        token_path=settings.gmail_token_path,
    )
    return ingest_metadata(
        engine=_postgres_engine,
        service=service,
        user_id=settings.gmail_user_id,
        page_size=settings.gmail_page_size,
        max_messages=max_messages,
    )


@app.post("/pipeline/cluster-label")
def run_cluster_label(max_clusters: int | None = None):
    settings = Settings()
    service = get_gmail_service_from_files(
        credentials_path=settings.gmail_credentials_path,
        token_path=settings.gmail_token_path,
    )
    return cluster_and_label(
        engine=_postgres_engine,
        service=service,
        user_id=settings.gmail_user_id,
        similarity_threshold=settings.similarity_threshold,
        label_version=settings.label_version,
        ollama_host=settings.ollama_host,
        ollama_model=settings.ollama_model,
        max_clusters=max_clusters,
    )
