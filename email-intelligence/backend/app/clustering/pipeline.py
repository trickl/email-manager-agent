"""Phase 2: clustering and Tier-1 labeling.

This phase runs after metadata ingestion. Bodies are fetched ONLY for representative samples.

Restart safety:
- Already-labelled emails are never relabelled.
- Cluster IDs are deterministic (seed + config), and cluster membership updates are idempotent.
"""

from __future__ import annotations

import logging
import random
import uuid

from app.clustering.analysis import frequency_label, unread_ratio_label
from app.gmail.client import get_message_body_text
from app.labeling.labeler import build_labeler
from app.labeling.tier1 import validate_tier1_category
from app.repository.email_query_repository import fetch_by_gmail_ids, fetch_next_unlabelled
from app.repository.email_query_repository import insert_cluster
from app.repository.email_query_repository import label_emails_in_cluster
from app.repository.email_query_repository import update_cluster_analysis
from app.repository.email_query_repository import update_cluster_label
from app.repository.pipeline_kv_repository import set_current_phase
from app.vector.embedding import build_embedding_text
from app.vector.qdrant import query_similar
from app.vector.vectorizer import vectorize_text
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


PHASE_NAME = "phase2_clustering_labeling"


def _cluster_id(*, seed_gmail_message_id: str, similarity_threshold: float, label_version: str) -> str:
    # Deterministic UUID: stable across restarts for the same configuration.
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"cluster:{seed_gmail_message_id}:{similarity_threshold}:{label_version}",
        )
    )


def _sample_count(cluster_size: int) -> int:
    if cluster_size > 50:
        return 4
    if 10 <= cluster_size <= 50:
        return 3
    if 5 <= cluster_size <= 10:
        return 2
    return 1


def cluster_and_label(
    *,
    engine,
    service,
    user_id: str,
    similarity_threshold: float,
    label_version: str,
    ollama_host: str | None,
    ollama_model: str,
    max_clusters: int | None = None,
    qdrant_limit: int = 200,
    progress_hook=None,
) -> dict[str, int]:
    """Iteratively cluster and label all unlabelled emails."""

    set_current_phase(engine, PHASE_NAME)
    labeler = build_labeler(ollama_host=ollama_host, ollama_model=ollama_model)

    clusters_done = 0
    emails_labeled = 0

    if progress_hook:
        progress_hook(clusters_done=clusters_done, emails_labeled=emails_labeled, message="Starting")

    while True:
        if max_clusters is not None and clusters_done >= max_clusters:
            break

        seed_row = fetch_next_unlabelled(engine)
        if seed_row is None:
            break

        seed = seed_row.email
        cluster_uuid = _cluster_id(
            seed_gmail_message_id=seed.gmail_message_id,
            similarity_threshold=similarity_threshold,
            label_version=label_version,
        )

        # Seed vector is deterministic from canonical metadata.
        seed_text = build_embedding_text(seed)
        seed_vector = vectorize_text(seed_text)

        # Phase 2A: Similarity search (filtered by sender domain)
        points = query_similar(
            seed_vector,
            from_domain=seed.from_domain,
            limit=qdrant_limit,
            score_threshold=similarity_threshold,
        )

        score_by_id: dict[str, float] = {}
        candidate_ids: list[str] = []

        # Always include seed.
        candidate_ids.append(seed.gmail_message_id)
        score_by_id[seed.gmail_message_id] = 1.0

        for p in points:
            payload = getattr(p, "payload", None) or {}
            mid = payload.get("gmail_message_id")
            if not mid:
                continue
            if mid not in score_by_id:
                score_by_id[mid] = float(getattr(p, "score", 0.0) or 0.0)
                candidate_ids.append(mid)

        rows = fetch_by_gmail_ids(engine, candidate_ids)

        # Phase 2A: consolidate cluster candidates.
        unlabelled_rows = [r for r in rows if r.category is None]

        cluster_ids: list[str] = []
        for r in unlabelled_rows:
            e = r.email
            if e.from_domain != seed.from_domain:
                continue

            score = score_by_id.get(e.gmail_message_id, 0.0)
            same_subject = (
                seed.subject_normalized
                and e.subject_normalized
                and e.subject_normalized == seed.subject_normalized
            )

            if same_subject or score >= similarity_threshold:
                cluster_ids.append(e.gmail_message_id)

        if not cluster_ids:
            cluster_ids = [seed.gmail_message_id]

        # Deterministic ordering for repeatability.
        rows_by_id = {r.email.gmail_message_id: r for r in rows}
        cluster_emails = [rows_by_id[i].email for i in cluster_ids if i in rows_by_id]
        cluster_emails.sort(key=lambda e: (e.internal_date, e.gmail_message_id))

        # Persist cluster identity (idempotent).
        insert_cluster(
            engine=engine,
            cluster_id=cluster_uuid,
            seed_gmail_message_id=seed.gmail_message_id,
            from_domain=seed.from_domain,
            subject_normalized=seed.subject_normalized,
            similarity_threshold=similarity_threshold,
            display_name=(seed.subject_normalized or seed.subject or f"Cluster {cluster_uuid[:8]}"),
        )

        # Phase 2B: representative body sampling
        sample_n = _sample_count(len(cluster_emails))
        rng_seed = int(uuid.UUID(cluster_uuid)) % (2**32)
        rng = random.Random(rng_seed)

        sample_ids = [e.gmail_message_id for e in cluster_emails]
        sampled = rng.sample(sample_ids, k=min(sample_n, len(sample_ids)))
        bodies: list[str] = []
        for mid in sampled:
            try:
                bodies.append(get_message_body_text(service, message_id=mid, user_id=user_id))
            except HttpError:
                logger.warning(
                    "gmail_body_fetch_failed",
                    extra={"gmail_message_id": mid, "phase": PHASE_NAME},
                    exc_info=True,
                )
            except Exception:
                logger.exception(
                    "gmail_body_fetch_failed_unexpected",
                    extra={"gmail_message_id": mid, "phase": PHASE_NAME},
                )

        # Phase 2B: frequency + unread analysis
        dates = [e.internal_date for e in cluster_emails]
        freq = frequency_label(dates)
        unread = unread_ratio_label([e.is_unread for e in cluster_emails])

        update_cluster_analysis(
            engine=engine,
            cluster_id=cluster_uuid,
            frequency_label=freq,
            unread_label=unread,
        )

        # Phase 2C: LLM labeling
        subject_examples = []
        seen = set()
        for e in cluster_emails:
            s = e.subject_normalized or ""
            if s and s not in seen:
                subject_examples.append(s)
                seen.add(s)
            if len(subject_examples) >= 5:
                break

        result = labeler.label(
            sender_domain=seed.from_domain,
            subject_examples=subject_examples,
            cluster_size=len(cluster_emails),
            frequency_label=freq,
            unread_label=unread,
            bodies=bodies,
        )

        category = validate_tier1_category(result.category)

        # Phase 2C: persist labeling results (never relabel already labelled)
        updated = label_emails_in_cluster(
            engine=engine,
            gmail_ids=[e.gmail_message_id for e in cluster_emails],
            cluster_id=cluster_uuid,
            category=category,
            subcategory=result.subcategory,
            label_confidence=result.confidence,
            label_version=label_version,
        )
        update_cluster_label(
            engine=engine,
            cluster_id=cluster_uuid,
            category=category,
            subcategory=result.subcategory,
            label_confidence=result.confidence,
            label_version=label_version,
        )

        clusters_done += 1
        emails_labeled += updated

        if progress_hook:
            progress_hook(
                clusters_done=clusters_done,
                emails_labeled=emails_labeled,
                message=f"Labelled cluster {clusters_done} ({updated} emails)",
            )

        if clusters_done % 20 == 0:
            logger.info(
                "clustering_progress",
                extra={"clusters_done": clusters_done, "emails_labeled": emails_labeled},
            )

    logger.info(
        "clustering_done",
        extra={"clusters_done": clusters_done, "emails_labeled": emails_labeled},
    )

    return {"clusters_done": clusters_done, "emails_labeled": emails_labeled}
