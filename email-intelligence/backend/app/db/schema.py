"""Runtime schema bootstrap for existing Postgres volumes.

`docker/postgres/init.sql` is only applied for fresh database volumes. In development (and during
restarts), we still need idempotent schema ensures so the backend can safely evolve.

This module focuses on the Email Intelligence pipeline tables/columns:
- pipeline_kv (checkpoint + current phase)
- email_cluster (cluster identity)
- labeling columns on email_message

Taxonomy schema/seed is handled separately in `app.repository.taxonomy_repository`.
"""

from __future__ import annotations

def ensure_core_schema(engine) -> None:
    """Ensure required tables/columns exist (idempotent).

    Args:
        engine: SQLAlchemy engine bound to the Postgres database.
    """

    from sqlalchemy import text

    ddl = text(
        """
        CREATE TABLE IF NOT EXISTS pipeline_kv (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS email_cluster (
            id UUID PRIMARY KEY,

            seed_gmail_message_id TEXT UNIQUE NOT NULL,
            from_domain TEXT NOT NULL,
            subject_normalized TEXT,
            similarity_threshold REAL NOT NULL,

            display_name TEXT,
            frequency_label TEXT,
            unread_label TEXT,

            category TEXT,
            subcategory TEXT,
            label_confidence REAL,
            label_version TEXT,

            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS category TEXT;
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS subcategory TEXT;
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS label_confidence REAL;
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS label_version TEXT;
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS cluster_id UUID;
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS label_ids TEXT[];

        CREATE INDEX IF NOT EXISTS idx_email_category
            ON email_message(category);

        CREATE INDEX IF NOT EXISTS idx_email_cluster_id
            ON email_message(cluster_id);

        CREATE INDEX IF NOT EXISTS idx_email_label_ids
            ON email_message USING GIN(label_ids);
        """
    )

    with engine.begin() as conn:
        conn.execute(ddl)
