"""Backfill real embeddings into Qdrant for already-ingested emails.

This is idempotent because Qdrant point IDs are deterministic (uuid5 of gmail_message_id).

Typical usage:
- Ensure Ollama is running and the embedding model exists locally (default: all-minilm)
- Run this script to upsert vectors for existing Postgres rows.

This script operates on *metadata only* (subject + from_domain), matching Phase 1 contract.
"""

from __future__ import annotations

import argparse
import time

# Ensure the backend package root (email-intelligence/backend) is on sys.path so
# `import app.*` works when this script is executed from other directories.
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from sqlalchemy import create_engine, text

from app.settings import Settings
from app.vector.embedding import build_embedding_text
from app.vector.qdrant import ensure_collection, upsert_email
from app.vector.vectorizer import vectorize_text


class _EmailRow:
    """Small adapter so build_embedding_text(email) works."""

    def __init__(self, *, gmail_message_id: str, subject_normalized: str | None, from_domain: str, is_unread: bool):
        self.gmail_message_id = gmail_message_id
        self.subject_normalized = subject_normalized
        self.from_domain = from_domain
        self.is_unread = is_unread


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill Qdrant vectors using Ollama embeddings")
    p.add_argument(
        "--start-id",
        type=int,
        default=0,
        help="Start processing from email_message.id > START_ID (useful for resume)",
    )
    p.add_argument("--limit", type=int, default=5000, help="Max rows to process")
    p.add_argument("--batch-size", type=int, default=200, help="DB page size")
    p.add_argument("--sleep-ms", type=int, default=0, help="Sleep between upserts to reduce load")
    p.add_argument(
        "--log-every",
        type=int,
        default=1000,
        help="Print a progress line every N processed rows (default: 1000)",
    )
    p.add_argument("--from-domain", type=str, default=None, help="Only process this sender domain")
    p.add_argument("--only-unlabelled", action="store_true", help="Only process rows where category is NULL")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    settings = Settings()

    ensure_collection()

    engine = create_engine(settings.database_url)

    processed = 0
    last_id = int(args.start_id)

    print(
        "starting_backfill",
        {
            "start_id": last_id,
            "limit": args.limit,
            "batch_size": args.batch_size,
            "sleep_ms": args.sleep_ms,
            "log_every": args.log_every,
            "from_domain": args.from_domain,
            "only_unlabelled": args.only_unlabelled,
            "embedding_model": settings.embedding_model,
        },
    )

    where = ["id > :last_id"]
    params: dict[str, object] = {"last_id": last_id}

    if args.from_domain:
        where.append("from_domain = :from_domain")
        params["from_domain"] = args.from_domain

    if args.only_unlabelled:
        where.append("category IS NULL")

    where_sql = " AND ".join(where)

    while processed < args.limit:
        params["last_id"] = last_id
        params["limit"] = min(args.batch_size, args.limit - processed)

        query = text(
            f"""
            SELECT id, gmail_message_id, subject_normalized, from_domain, is_unread
            FROM email_message
            WHERE {where_sql}
            ORDER BY id ASC
            LIMIT :limit
            """
        )

        with engine.begin() as conn:
            rows = conn.execute(query, params).mappings().all()

        if not rows:
            break

        for r in rows:
            last_id = int(r["id"])
            email = _EmailRow(
                gmail_message_id=str(r["gmail_message_id"]),
                subject_normalized=r["subject_normalized"],
                from_domain=str(r["from_domain"]),
                is_unread=bool(r["is_unread"]),
            )
            embedding_text = build_embedding_text(email)
            vector = vectorize_text(embedding_text)
            upsert_email(email, vector)

            processed += 1
            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000.0)

            if args.log_every > 0 and processed % args.log_every == 0:
                print(f"processed={processed} last_id={last_id}")

    print(f"done processed={processed} last_id={last_id}")


if __name__ == "__main__":
    main()
