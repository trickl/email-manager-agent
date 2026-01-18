"""Phase 1: metadata-only ingestion.

Ingests all messages across all folders/labels (optionally incremental) and stores:
- Canonical metadata in Postgres
- Deterministic vectors in Qdrant

Bodies are NOT fetched in this phase.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.gmail.client import get_message_metadata, iter_message_ids
from app.gmail.mapping import metadata_to_domain
from app.repository.email_repository import insert_email
from app.repository.pipeline_kv_repository import get_checkpoint_internal_date
from app.repository.pipeline_kv_repository import set_checkpoint_internal_date
from app.repository.pipeline_kv_repository import set_current_phase
from app.vector.embedding import build_embedding_text
from app.vector.qdrant import upsert_email
from app.vector.vectorizer import vectorize_text

logger = logging.getLogger(__name__)


PHASE_NAME = "phase1_metadata_ingestion"


def _query_after(checkpoint: datetime | None) -> str | None:
    if not checkpoint:
        return None

    # Gmail supports `after:<unix_seconds>`.
    # Use a small safety window to avoid boundary misses.
    if checkpoint.tzinfo is None:
        checkpoint = checkpoint.replace(tzinfo=timezone.utc)

    safe = checkpoint - timedelta(seconds=1)
    return f"after:{int(safe.timestamp())}"


def ingest_metadata(
    *,
    engine,
    service,
    user_id: str = "me",
    page_size: int = 500,
    max_messages: int | None = None,
    progress_hook=None,
) -> dict[str, int | str | None]:
    """Run metadata-only ingestion.

    Mandatory order per message:
    1) Insert into Postgres
    2) Build embedding text
    3) Generate vector
    4) Upsert into Qdrant

    The checkpoint is only advanced after step (4) succeeds.

    Returns:
        Summary dict for observability.
    """

    set_current_phase(engine, PHASE_NAME)

    checkpoint = get_checkpoint_internal_date(engine)
    q = _query_after(checkpoint)

    logger.info(
        "metadata_ingestion_start",
        extra={
            "checkpoint": checkpoint.isoformat() if checkpoint else None,
            "query": q,
        },
    )

    processed = 0
    skipped = 0
    failed = 0
    advanced_to: datetime | None = checkpoint

    if progress_hook:
        progress_hook(processed=processed, skipped=skipped, failed=failed, message="Starting")

    for msg_id in iter_message_ids(service, user_id=user_id, page_size=page_size, q=q):
        if max_messages is not None and processed >= max_messages:
            break

        try:
            meta = get_message_metadata(service, message_id=msg_id, user_id=user_id)
            email = metadata_to_domain(meta)

            # If we used a safety window in the query, filter explicitly.
            if checkpoint is not None and email.internal_date <= checkpoint:
                skipped += 1
                if progress_hook:
                    progress_hook(
                        processed=processed,
                        skipped=skipped,
                        failed=failed,
                        message="Skipping already-ingested message",
                    )
                continue

            # 1. Persist metadata (canonical)
            insert_email(email)

            # 2. Build stable embedding input text (contract)
            embedding_text = build_embedding_text(email)

            # 3. Generate deterministic vector
            vector = vectorize_text(embedding_text)

            # 4. Upsert into vector DB
            upsert_email(email, vector)

            processed += 1

            if progress_hook:
                progress_hook(
                    processed=processed,
                    skipped=skipped,
                    failed=failed,
                    message=f"Ingested metadata for message {processed}",
                )

            # Advance checkpoint only after Qdrant upsert succeeds.
            if advanced_to is None or email.internal_date > advanced_to:
                advanced_to = email.internal_date
                set_checkpoint_internal_date(engine, advanced_to)

            if processed % 250 == 0:
                logger.info(
                    "metadata_ingestion_progress",
                    extra={
                        "processed": processed,
                        "skipped": skipped,
                        "failed": failed,
                        "checkpoint": advanced_to.isoformat() if advanced_to else None,
                    },
                )

        except Exception as exc:  # noqa: BLE001 - pipeline should continue
            failed += 1
            logger.exception(
                "metadata_ingestion_message_failed",
                extra={"gmail_message_id": msg_id, "error": str(exc)},
            )

            if progress_hook:
                progress_hook(
                    processed=processed,
                    skipped=skipped,
                    failed=failed,
                    message=f"Failed message {msg_id}",
                )

    logger.info(
        "metadata_ingestion_done",
        extra={
            "processed": processed,
            "skipped": skipped,
            "failed": failed,
            "checkpoint": advanced_to.isoformat() if advanced_to else None,
        },
    )

    return {
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "checkpoint": advanced_to.isoformat() if advanced_to else None,
    }
