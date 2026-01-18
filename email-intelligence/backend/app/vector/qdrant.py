"""Qdrant client stub.

Phase 0 requirement: connect to Qdrant and ensure an empty collection exists.
"""

from __future__ import annotations

import uuid
from functools import lru_cache

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, FieldCondition, Filter, MatchValue, PointStruct, VectorParams

from app.settings import Settings
from app.vector.embedding import build_embedding_text
from app.vector.vectorizer import VECTOR_SIZE, vector_version_tag

COLLECTION_NAME = "email_subjects"


@lru_cache
def _settings() -> Settings:
    return Settings()


@lru_cache
def _client() -> QdrantClient:
    s = _settings()
    return QdrantClient(host=s.qdrant_host, port=s.qdrant_port)


def ensure_collection() -> None:
    client = _client()
    collections = client.get_collections().collections
    names = [c.name for c in collections]

    if COLLECTION_NAME not in names:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=VECTOR_SIZE,
                distance=Distance.COSINE,
            ),
        )
        return

    # Validate vector dimension so we don't silently write/query incompatible vectors.
    info = client.get_collection(COLLECTION_NAME)
    # Qdrant returns different shapes depending on single vs named vectors; handle single.
    size = None
    try:
        size = info.config.params.vectors.size  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - tolerate API differences
        size = None

    if size is not None and int(size) != int(VECTOR_SIZE):
        raise RuntimeError(
            f"Qdrant collection '{COLLECTION_NAME}' has vector size {size}, but code expects {VECTOR_SIZE}. "
            "Either migrate/recreate the collection or use an embedding model with matching dimensions."
        )


def upsert_email(email, vector):
    # Contract: build_embedding_text is the stable embedding input format.
    # (Embedding generation itself is stubbed in Phase 1.)
    _text = build_embedding_text(email)

    point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, email.gmail_message_id))

    version = vector_version_tag()

    client = _client()
    client.upsert(
        collection_name=COLLECTION_NAME,
        points=[
            PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "gmail_message_id": email.gmail_message_id,
                    "from_domain": email.from_domain,
                    "subject_normalized": email.subject_normalized,
                    "is_unread": email.is_unread,
                    "vector_version": version,
                },
            )
        ],
    )


def point_id_for_gmail_message_id(gmail_message_id: str) -> str:
    """Return the deterministic Qdrant point id for a Gmail message id."""

    return str(uuid.uuid5(uuid.NAMESPACE_URL, gmail_message_id))


def get_vector_for_gmail_message_id(gmail_message_id: str) -> list[float] | None:
    """Fetch an existing vector from Qdrant for this Gmail message.

    This lets clustering reuse stored embeddings rather than recomputing them.
    """

    client = _client()
    pid = point_id_for_gmail_message_id(gmail_message_id)
    records = client.retrieve(
        collection_name=COLLECTION_NAME,
        ids=[pid],
        with_payload=False,
        with_vectors=True,
    )
    if not records:
        return None

    vec = getattr(records[0], "vector", None)
    if vec is None:
        return None

    # Qdrant can return either a list (single unnamed vector) or a dict for named vectors.
    if isinstance(vec, dict):
        if not vec:
            return None
        # Pick the first named vector.
        vec = next(iter(vec.values()))

    if not isinstance(vec, list):
        return None
    return [float(x) for x in vec]


def query_similar(
    vector,
    *,
    from_domain: str | None = None,
    limit: int = 5,
    score_threshold=None,
    vector_version: str | None = None,
):
    client = _client()
    query_filter = None

    must = []
    if from_domain:
        must.append(FieldCondition(key="from_domain", match=MatchValue(value=from_domain)))
    if vector_version:
        must.append(FieldCondition(key="vector_version", match=MatchValue(value=vector_version)))
    if must:
        query_filter = Filter(must=must)

    response = client.query_points(
        collection_name=COLLECTION_NAME,
        query=vector,
        query_filter=query_filter,
        limit=limit,
        score_threshold=score_threshold,
        with_payload=True,
    )
    return response.points
