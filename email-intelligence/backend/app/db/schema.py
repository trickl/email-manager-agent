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

        -- Stage 1: lifecycle state (non-destructive-by-default)
        -- Source of truth lives in Postgres; provider labels/folders are not sufficient.
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS lifecycle_state TEXT NOT NULL DEFAULT 'ACTIVE';
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS trashed_at TIMESTAMP;
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS expiry_at TIMESTAMP;
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS trashed_by_policy_id UUID;

        CREATE INDEX IF NOT EXISTS idx_email_lifecycle_state
            ON email_message(lifecycle_state);
        CREATE INDEX IF NOT EXISTS idx_email_expiry_at
            ON email_message(expiry_at);

        -- Stage 2: deterministic policy engine (rules v1)
        CREATE TABLE IF NOT EXISTS email_policy (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,

            -- 'scheduled' or 'on_ingest' (Stage 2 focuses on scheduled/batch)
            trigger_type TEXT NOT NULL DEFAULT 'scheduled',
            -- A human-friendly cadence hint; we start with weekly evaluation.
            cadence TEXT NOT NULL DEFAULT 'weekly',

            -- JSON config so we can evolve conditions/actions without migrations.
            -- Expected shape documented in app/policy/models.py.
            definition_json JSONB NOT NULL,

            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_email_policy_enabled
            ON email_policy(enabled);

        -- Audit log for reversible actions.
        CREATE TABLE IF NOT EXISTS email_action_log (
            id UUID PRIMARY KEY,
            action_type TEXT NOT NULL,
            gmail_message_id TEXT,
            policy_id UUID,
            status TEXT NOT NULL DEFAULT 'succeeded',
            error TEXT,

            occurred_at TIMESTAMP NOT NULL DEFAULT NOW(),

            -- Snapshot of key message attributes at the time of action.
            from_domain TEXT,
            subject TEXT,
            internal_date TIMESTAMP,

            -- State deltas (optional, but useful for audits)
            from_state TEXT,
            to_state TEXT,
            trashed_at TIMESTAMP,
            expiry_at TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_email_action_log_occurred_at
            ON email_action_log(occurred_at);
        CREATE INDEX IF NOT EXISTS idx_email_action_log_policy
            ON email_action_log(policy_id);
        """
    )

    with engine.begin() as conn:
        conn.execute(ddl)
