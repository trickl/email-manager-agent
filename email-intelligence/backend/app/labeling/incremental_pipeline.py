"""Incremental (daily) per-email labeling pipeline.

Bulk mode (Phase 2) clusters similar emails and labels many at once using representative body samples.

Incremental mode is optimized for small daily deltas:
- Phase 1 ingests metadata only (no bodies)
- Phase 2 labels *each* unlabelled email individually (fetching just that email's body)

Important: we reuse the exact same prompt contract as bulk mode to avoid prompt drift.
"""

from __future__ import annotations

import logging
import uuid

from googleapiclient.errors import HttpError

from app.clustering.analysis import frequency_label, unread_ratio_label
from app.gmail.client import get_message_body_text
from app.labeling.labeler import build_labeler
from app.labeling.tier1 import validate_tier1_category
from app.repository.email_query_repository import fetch_next_unlabelled
from app.repository.email_query_repository import fetch_recent_domain_activity
from app.repository.email_query_repository import insert_cluster
from app.repository.email_query_repository import label_emails_in_cluster
from app.repository.email_query_repository import update_cluster_analysis
from app.repository.email_query_repository import update_cluster_label
from app.repository.pipeline_kv_repository import set_current_phase
from app.repository.taxonomy_repository import ensure_taxonomy_seeded
from app.repository.taxonomy_repository import ensure_tier2_label
from app.repository.taxonomy_repository import list_tier2_options
from app.repository.taxonomy_assignment_repository import upsert_message_taxonomy_assignment

logger = logging.getLogger(__name__)

PHASE_NAME = "phase2_incremental_labeling"


def _cluster_id(*, seed_gmail_message_id: str, similarity_threshold: float, label_version: str) -> str:
    """Deterministic cluster id for a single-email "cluster".

    We intentionally use the same deterministic UUID scheme as bulk mode so that:
    - reruns are idempotent
    - if a later bulk run chooses the same seed email, it naturally converges
    """

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"cluster:{seed_gmail_message_id}:{similarity_threshold}:{label_version}",
        )
    )


def label_unlabelled_individual(
    *,
    engine,
    service,
    user_id: str,
    similarity_threshold: float,
    label_version: str,
    ollama_host: str | None,
    ollama_model: str,
    max_emails: int | None = None,
    progress_hook=None,
) -> dict[str, int]:
    """Label each currently-unlabelled email one-by-one.

    Args:
        engine: SQLAlchemy engine.
        service: Gmail API service.
        user_id: Gmail user id, typically "me".
        similarity_threshold: Kept for deterministic cluster id compatibility.
        label_version: Stored for provenance.
        ollama_host: Ollama base URL.
        ollama_model: Ollama model name for labeling.
        max_emails: Optional cap (useful for daily runs).
        progress_hook: Optional callback for job progress.

    Returns:
        Summary dict with counters.
    """

    ensure_taxonomy_seeded(engine)
    set_current_phase(engine, PHASE_NAME)

    labeler = build_labeler(ollama_host=ollama_host, ollama_model=ollama_model)

    emails_processed = 0
    emails_labeled = 0
    emails_failed = 0

    if progress_hook:
        progress_hook(
            emails_processed=emails_processed,
            emails_labeled=emails_labeled,
            emails_failed=emails_failed,
            message="Starting",
        )

    while True:
        if max_emails is not None and emails_processed >= max_emails:
            break

        row = fetch_next_unlabelled(engine)
        if row is None:
            break

        email = row.email
        cluster_uuid = _cluster_id(
            seed_gmail_message_id=email.gmail_message_id,
            similarity_threshold=similarity_threshold,
            label_version=label_version,
        )

        try:
            # Persist a single-email cluster record (idempotent).
            insert_cluster(
                engine=engine,
                cluster_id=cluster_uuid,
                seed_gmail_message_id=email.gmail_message_id,
                from_domain=email.from_domain,
                subject_normalized=email.subject_normalized,
                similarity_threshold=similarity_threshold,
                display_name=(email.subject_normalized or email.subject or f"Email {email.gmail_message_id}"),
            )

            # Compute lightweight domain context (no bodies).
            dates, unread_flags = fetch_recent_domain_activity(engine, from_domain=email.from_domain, limit=30)
            freq = frequency_label(dates if dates else [email.internal_date])
            unread = unread_ratio_label(unread_flags if unread_flags else [email.is_unread])

            update_cluster_analysis(
                engine=engine,
                cluster_id=cluster_uuid,
                frequency_label=freq,
                unread_label=unread,
            )

            # Fetch body for this specific email only.
            body = get_message_body_text(service, message_id=email.gmail_message_id, user_id=user_id)

            subject_examples: list[str] = []
            s = (email.subject_normalized or email.subject or "").strip()
            if s:
                subject_examples.append(s)

            # Fetch latest Tier-2 options so prompt stays aligned with current taxonomy.
            tier2 = list_tier2_options(engine)

            result = labeler.label(
                sender_domain=email.from_domain,
                subject_examples=subject_examples,
                cluster_size=1,
                frequency_label=freq,
                unread_label=unread,
                bodies=[body],
                tier2_options=tier2,
            )

            category = validate_tier1_category(result.category)
            subcategory = result.subcategory
            if subcategory is not None:
                subcategory = str(subcategory).strip() or None

            if subcategory is not None and subcategory not in set(tier2.get(category, [])):
                ensure_tier2_label(engine, category_name=category, subcategory_name=subcategory)

            updated = label_emails_in_cluster(
                engine=engine,
                gmail_ids=[email.gmail_message_id],
                cluster_id=cluster_uuid,
                category=category,
                subcategory=subcategory,
                label_version=label_version,
            )

            # Persist taxonomy assignment and enqueue incremental Gmail sync.
            upsert_message_taxonomy_assignment(
                engine=engine,
                gmail_message_id=email.gmail_message_id,
                category=category,
                subcategory=subcategory,
                confidence=None,
            )

            update_cluster_label(
                engine=engine,
                cluster_id=cluster_uuid,
                category=category,
                subcategory=subcategory,
                label_version=label_version,
            )

            emails_processed += 1
            emails_labeled += updated

            if progress_hook:
                progress_hook(
                    emails_processed=emails_processed,
                    emails_labeled=emails_labeled,
                    emails_failed=emails_failed,
                    message=f"Labelled {email.gmail_message_id} ({updated} updated)",
                )

        except HttpError:
            emails_processed += 1
            emails_failed += 1
            logger.warning(
                "gmail_body_fetch_failed",
                extra={"gmail_message_id": email.gmail_message_id, "phase": PHASE_NAME},
                exc_info=True,
            )
            if progress_hook:
                progress_hook(
                    emails_processed=emails_processed,
                    emails_labeled=emails_labeled,
                    emails_failed=emails_failed,
                    message=f"Failed body fetch {email.gmail_message_id}",
                )
        except Exception as exc:  # noqa: BLE001
            emails_processed += 1
            emails_failed += 1
            logger.exception(
                "incremental_label_failed",
                extra={"gmail_message_id": email.gmail_message_id, "error": str(exc)},
            )
            if progress_hook:
                progress_hook(
                    emails_processed=emails_processed,
                    emails_labeled=emails_labeled,
                    emails_failed=emails_failed,
                    message=f"Failed label {email.gmail_message_id}",
                )

    logger.info(
        "incremental_label_done",
        extra={
            "emails_processed": emails_processed,
            "emails_labeled": emails_labeled,
            "emails_failed": emails_failed,
        },
    )

    return {
        "emails_processed": emails_processed,
        "emails_labeled": emails_labeled,
        "emails_failed": emails_failed,
    }
