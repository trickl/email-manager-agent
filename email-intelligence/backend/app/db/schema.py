"""Runtime schema bootstrap for existing Postgres volumes.

`docker/postgres/init.sql` is only applied for fresh database volumes. In development (and during
restarts), we still need idempotent schema ensures so the backend can safely evolve.

This module focuses on the Email Intelligence pipeline tables/columns:
- pipeline_kv (checkpoints + watermarks)
- email_cluster (cluster identity)
- labeling columns on email_message
- taxonomy assignment + Gmail sync outbox tables

Taxonomy schema/seed itself is handled separately in `app.repository.taxonomy_repository`.
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

        -- Hygiene: retention-driven archiving (Gmail INBOX removal + Archive label).
        ALTER TABLE email_message
            ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP;

        CREATE INDEX IF NOT EXISTS idx_email_archived_at
            ON email_message(archived_at);

        CREATE INDEX IF NOT EXISTS idx_email_category
            ON email_message(category);

        CREATE INDEX IF NOT EXISTS idx_email_cluster_id
            ON email_message(cluster_id);

        CREATE INDEX IF NOT EXISTS idx_email_label_ids
            ON email_message USING GIN(label_ids);

        -- Event extraction (Phase 2+): per-message structured event metadata.
        -- One row per email_message; idempotent updates allow re-running the extractor.
        CREATE TABLE IF NOT EXISTS message_event_metadata (
            message_id INTEGER PRIMARY KEY REFERENCES email_message(id) ON DELETE CASCADE,

            status TEXT NOT NULL DEFAULT 'queued',
            error TEXT,

            event_name TEXT,
            event_type TEXT,
            event_date DATE,
            start_time TIME,
            end_time TIME,
            timezone TEXT,
            end_time_inferred BOOLEAN NOT NULL DEFAULT FALSE,

            confidence REAL,
            model TEXT,
            prompt_version TEXT,
            raw_json JSONB,

            extracted_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        -- Optional UX / integration fields.
        ALTER TABLE message_event_metadata
            ADD COLUMN IF NOT EXISTS hidden_at TIMESTAMPTZ;
        ALTER TABLE message_event_metadata
            ADD COLUMN IF NOT EXISTS calendar_event_id TEXT;
        ALTER TABLE message_event_metadata
            ADD COLUMN IF NOT EXISTS calendar_ical_uid TEXT;
        ALTER TABLE message_event_metadata
            ADD COLUMN IF NOT EXISTS calendar_checked_at TIMESTAMPTZ;
        ALTER TABLE message_event_metadata
            ADD COLUMN IF NOT EXISTS calendar_published_at TIMESTAMPTZ;

        CREATE INDEX IF NOT EXISTS idx_message_event_metadata_status
            ON message_event_metadata(status);
        CREATE INDEX IF NOT EXISTS idx_message_event_metadata_event_date
            ON message_event_metadata(event_date);

        CREATE INDEX IF NOT EXISTS idx_message_event_metadata_hidden_at
            ON message_event_metadata(hidden_at);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_message_event_metadata_calendar_ical_uid
            ON message_event_metadata(calendar_ical_uid);

        -- Best-effort guardrail: constrain event_type to the allowed set.
        -- Use NOT VALID so existing rows from earlier prompt versions don't block startup.
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'chk_message_event_metadata_event_type'
            ) THEN
                ALTER TABLE message_event_metadata
                    ADD CONSTRAINT chk_message_event_metadata_event_type
                    CHECK (
                        event_type IS NULL
                        OR event_type IN ('Theatre', 'Comedy', 'Opera', 'Ballet', 'Cinema', 'Social', 'Other')
                    ) NOT VALID;
            END IF;
        END $$;

        -- Payment extraction: per-message structured payment metadata.
        -- One row per email_message; idempotent updates allow re-running the extractor.
        CREATE TABLE IF NOT EXISTS message_payment_metadata (
            message_id INTEGER PRIMARY KEY REFERENCES email_message(id) ON DELETE CASCADE,

            status TEXT NOT NULL DEFAULT 'queued',
            error TEXT,

            item_name TEXT,
            vendor_name TEXT,
            item_category TEXT,

            cost_amount NUMERIC(12, 2),
            cost_currency TEXT,

            is_recurring BOOLEAN,
            frequency TEXT,
            payment_date DATE,

            payment_fingerprint TEXT,

            confidence REAL,
            model TEXT,
            prompt_version TEXT,
            raw_json JSONB,

            extracted_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        ALTER TABLE message_payment_metadata
            ADD COLUMN IF NOT EXISTS item_category TEXT;

        CREATE INDEX IF NOT EXISTS idx_message_payment_metadata_status
            ON message_payment_metadata(status);
        CREATE INDEX IF NOT EXISTS idx_message_payment_metadata_payment_date
            ON message_payment_metadata(payment_date);
        CREATE INDEX IF NOT EXISTS idx_message_payment_metadata_vendor_name
            ON message_payment_metadata(vendor_name);
        CREATE INDEX IF NOT EXISTS idx_message_payment_metadata_item_category
            ON message_payment_metadata(item_category);
        CREATE INDEX IF NOT EXISTS idx_message_payment_metadata_fingerprint
            ON message_payment_metadata(payment_fingerprint);

        -- Taxonomy assignment: DB is source-of-truth for message->label mapping.
        CREATE TABLE IF NOT EXISTS message_taxonomy_label (
            message_id INTEGER NOT NULL REFERENCES email_message(id) ON DELETE CASCADE,
            taxonomy_label_id INTEGER NOT NULL REFERENCES taxonomy_label(id) ON DELETE CASCADE,

            assigned_at TIMESTAMP NOT NULL DEFAULT NOW(),
            confidence REAL,

            PRIMARY KEY (message_id, taxonomy_label_id)
        );

        CREATE INDEX IF NOT EXISTS idx_message_taxonomy_label_taxonomy
            ON message_taxonomy_label(taxonomy_label_id);
        CREATE INDEX IF NOT EXISTS idx_message_taxonomy_label_message
            ON message_taxonomy_label(message_id);

        -- Gmail sync outbox: supports efficient incremental push without rescanning.
        CREATE TABLE IF NOT EXISTS label_push_outbox (
            id BIGSERIAL PRIMARY KEY,
            message_id INTEGER NOT NULL REFERENCES email_message(id) ON DELETE CASCADE,
            reason TEXT NOT NULL DEFAULT 'label_assigned',
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMP,
            error TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_label_push_outbox_created_at
            ON label_push_outbox(created_at);
        CREATE INDEX IF NOT EXISTS idx_label_push_outbox_processed_at
            ON label_push_outbox(processed_at);

        -- Retention archive outbox: supports a two-phase "plan then push" flow.
        -- We keep this separate from taxonomy label push because it's a special marker label.
        CREATE TABLE IF NOT EXISTS archive_push_outbox (
            id BIGSERIAL PRIMARY KEY,
            message_id INTEGER NOT NULL REFERENCES email_message(id) ON DELETE CASCADE,
            reason TEXT NOT NULL DEFAULT 'retention_eligible',
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMP,
            error TEXT,
            UNIQUE (message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_archive_push_outbox_created_at
            ON archive_push_outbox(created_at);
        CREATE INDEX IF NOT EXISTS idx_archive_push_outbox_processed_at
            ON archive_push_outbox(processed_at);
        """
    )

    with engine.begin() as conn:
        conn.execute(ddl)
