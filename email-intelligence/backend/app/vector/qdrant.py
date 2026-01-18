"""Qdrant client stub.

Phase 0 requirement: connect to Qdrant and ensure an empty collection exists.
"""

from __future__ import annotations

import os
import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, FieldCondition, Filter, MatchValue, PointStruct, VectorParams

from app.vector.embedding import build_embedding_text
from app.vector.vectorizer import VECTOR_SIZE

QDRANT_HOST = os.getenv(
    "EMAIL_INTEL_QDRANT_HOST",
    os.getenv("QDRANT_HOST", "localhost"),
)
QDRANT_PORT = int(
    os.getenv(
        "EMAIL_INTEL_QDRANT_PORT",
        os.getenv("QDRANT_PORT", "6333"),
    )
)

client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

COLLECTION_NAME = "email_subjects"


def ensure_collection() -> None:
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


def upsert_email(email, vector):
    # Contract: build_embedding_text is the stable embedding input format.
    # (Embedding generation itself is stubbed in Phase 1.)
    _text = build_embedding_text(email)

    point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, email.gmail_message_id))

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
                },
            )
        ],
    )


def query_similar(vector, *, from_domain: str | None = None, limit: int = 5, score_threshold=None):
    query_filter = None
    if from_domain:
        query_filter = Filter(
            must=[
                FieldCondition(
                    key="from_domain",
                    match=MatchValue(value=from_domain),
                )
            ]
        )

    response = client.query_points(
        collection_name=COLLECTION_NAME,
        query=vector,
        query_filter=query_filter,
        limit=limit,
        score_threshold=score_threshold,
        with_payload=True,
    )
    return response.points
