"""Phase 2: clustering and Tier-1 labeling.

This phase runs after metadata ingestion. Bodies are fetched ONLY for representative samples.

Restart safety:
- Already-labelled emails are never relabelled.
- Cluster IDs are deterministic (seed + config), and cluster membership updates are idempotent.
"""

from __future__ import annotations

import logging
import random
import re
import uuid

from app.clustering.analysis import frequency_label, unread_ratio_label
from app.gmail.client import get_message_body_text
from app.labeling.labeler import build_labeler
from app.labeling.tier1 import validate_tier1_category
from app.repository.email_query_repository import fetch_by_gmail_ids, fetch_next_unlabelled
from app.repository.email_query_repository import fetch_unlabelled_by_domain
from app.repository.email_query_repository import insert_cluster
from app.repository.email_query_repository import label_emails_in_cluster
from app.repository.email_query_repository import update_cluster_analysis
from app.repository.email_query_repository import update_cluster_label
from app.repository.taxonomy_repository import ensure_taxonomy_seeded
from app.repository.taxonomy_repository import ensure_tier2_label
from app.repository.taxonomy_repository import list_tier2_options
from app.repository.pipeline_kv_repository import set_current_phase
from app.vector.embedding import build_embedding_text
from app.vector.qdrant import get_vector_for_gmail_message_id
from app.vector.qdrant import query_similar
from app.vector.vectorizer import vectorize_text
from app.vector.vectorizer import vector_version_tag
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


PHASE_NAME = "phase2_clustering_labeling"


_WORD_RE = re.compile(r"[a-z0-9]+", re.I)


def _tokenize_subject(s: str) -> set[str]:
    if not s:
        return set()

    tokens = {t.lower() for t in _WORD_RE.findall(s)}
    stop = {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "for",
        "from",
        "has",
        "have",
        "in",
        "is",
        "it",
        "of",
        "on",
        "or",
        "re",
        "the",
        "to",
        "your",
    }
    return {t for t in tokens if t not in stop and len(t) >= 3}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    union = len(a | b)
    if union == 0:
        return 0.0
    return inter / union


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

    # Ensure taxonomy tables are present before labeling begins.
    ensure_taxonomy_seeded(engine)

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

        # Phase 2A: Cluster candidates
        #
        # We intentionally batch label many emails per iteration. We first cluster by:
        #   - same sender domain
        #   - similar subject tokens (Jaccard overlap)
        # and only fall back to Qdrant similarity if this produces only the seed.

        domain_rows = fetch_unlabelled_by_domain(engine, from_domain=seed.from_domain, limit=2000)
        seed_tokens = _tokenize_subject(seed.subject_normalized or seed.subject or "")

        cluster_emails = [seed]
        seen_ids = {seed.gmail_message_id}

        if seed_tokens:
            for r in domain_rows:
                e = r.email
                if e.gmail_message_id in seen_ids:
                    continue

                et = _tokenize_subject(e.subject_normalized or e.subject or "")
                if not et:
                    continue

                # Modest threshold because sender-domain is already a strong constraint.
                # 0.20 is intentionally permissive: we prefer larger, coherent sender+subject clusters
                # over 1-email clusters when subjects are clearly related.
                if _jaccard(seed_tokens, et) >= 0.20:
                    cluster_emails.append(e)
                    seen_ids.add(e.gmail_message_id)

        if len(cluster_emails) == 1:
            # Use the stored Qdrant vector (fast) instead of recomputing embeddings per seed.
            seed_vector = get_vector_for_gmail_message_id(seed.gmail_message_id)
            if seed_vector is None:
                # Should be rare after backfill; keep a safe fallback.
                seed_text = build_embedding_text(seed)
                seed_vector = vectorize_text(seed_text)

            # Filter to current vector provenance so we don't match against legacy dummy vectors.
            version = vector_version_tag()

            # Similarity search (filtered by sender domain)
            points = query_similar(
                seed_vector,
                from_domain=seed.from_domain,
                limit=qdrant_limit,
                score_threshold=similarity_threshold,
                vector_version=version,
            )

            candidate_ids = [seed.gmail_message_id]
            for p in points:
                payload = getattr(p, "payload", None) or {}
                mid = payload.get("gmail_message_id")
                if mid and mid not in seen_ids:
                    candidate_ids.append(mid)
                    seen_ids.add(mid)

            rows = fetch_by_gmail_ids(engine, candidate_ids)
            rows_by_id = {r.email.gmail_message_id: r for r in rows}
            cluster_emails = [rows_by_id[i].email for i in candidate_ids if i in rows_by_id]

        # Deterministic ordering for repeatability.
        cluster_emails.sort(key=lambda e: (e.internal_date, e.gmail_message_id))

        # Safety cap for large sender domains.
        if len(cluster_emails) > 500:
            cluster_emails = cluster_emails[:500]

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

        # Fetch latest Tier-2 options each iteration so prompt stays up-to-date even if we extend.
        tier2 = list_tier2_options(engine)

        result = labeler.label(
            sender_domain=seed.from_domain,
            subject_examples=subject_examples,
            cluster_size=len(cluster_emails),
            frequency_label=freq,
            unread_label=unread,
            bodies=bodies,
            tier2_options=tier2,
        )

        category = validate_tier1_category(result.category)

        # Tier-2 is preferred (not strictly enforced): if the model proposes a new subcategory,
        # persist it so future prompts always include the latest taxonomy.
        subcategory = result.subcategory
        if subcategory is not None:
            subcategory = str(subcategory).strip() or None
        if subcategory is not None and subcategory not in set(tier2.get(category, [])):
            ensure_tier2_label(engine, category_name=category, subcategory_name=subcategory)

        # Phase 2C: persist labeling results (never relabel already labelled)
        updated = label_emails_in_cluster(
            engine=engine,
            gmail_ids=[e.gmail_message_id for e in cluster_emails],
            cluster_id=cluster_uuid,
            category=category,
            subcategory=subcategory,
            label_version=label_version,
        )
        update_cluster_label(
            engine=engine,
            cluster_id=cluster_uuid,
            category=category,
            subcategory=subcategory,
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
