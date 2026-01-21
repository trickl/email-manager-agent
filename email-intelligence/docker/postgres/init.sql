CREATE TABLE IF NOT EXISTS email_message (
    id SERIAL PRIMARY KEY,

    gmail_message_id TEXT UNIQUE NOT NULL,
    thread_id TEXT,

    subject TEXT,
    subject_normalized TEXT,

    from_address TEXT NOT NULL,
    from_domain TEXT NOT NULL,

    to_addresses TEXT[],
    cc_addresses TEXT[],
    bcc_addresses TEXT[],

    is_unread BOOLEAN NOT NULL,
    internal_date TIMESTAMP NOT NULL,

    -- Gmail label IDs (system + user labels). Represents folder/label membership.
    label_ids TEXT[],

    -- Set when retention sweep archives a message (removes INBOX and adds Archive label).
    archived_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_archived_at
    ON email_message(archived_at);

-- Pipeline state/checkpoint (simple KV store)
CREATE TABLE IF NOT EXISTS pipeline_kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Clustering output (cluster identity and summary metadata).
-- Cluster membership is stored on `email_message.cluster_id`.
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

-- Add labeling + clustering columns to canonical store.
-- (Idempotent for repeated init runs.)
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

CREATE INDEX IF NOT EXISTS idx_email_category
    ON email_message(category);

CREATE INDEX IF NOT EXISTS idx_email_cluster_id
    ON email_message(cluster_id);

CREATE INDEX IF NOT EXISTS idx_email_label_ids
    ON email_message USING GIN(label_ids);

-- Tiered taxonomy labels (hierarchy-ready).
-- NOTE: There is intentionally no "unknown" label. If a cluster doesn't fit,
-- create a new taxonomical label rather than defaulting to "Unknown".
CREATE TABLE IF NOT EXISTS taxonomy_label (
    id SERIAL PRIMARY KEY,

    level SMALLINT NOT NULL CHECK (level >= 1),
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',

    parent_id INTEGER REFERENCES taxonomy_label(id) ON DELETE RESTRICT,

    -- Retention duration in days; when an assigned label is older than this, retention sweep archives.
    retention_days INTEGER,

    -- Admin controls / safety.
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    managed_by_system BOOLEAN NOT NULL DEFAULT TRUE,

    -- Gmail label mapping (kept in sync by label sync jobs).
    gmail_label_id TEXT,
    last_sync_at TIMESTAMP,
    sync_status TEXT,
    sync_error TEXT,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_taxonomy_label_level
    ON taxonomy_label(level);

CREATE INDEX IF NOT EXISTS idx_taxonomy_label_parent
    ON taxonomy_label(parent_id);

-- Pre-seeded Tier-1 taxonomy (enforced)
INSERT INTO taxonomy_label (level, slug, name, description, parent_id, retention_days, is_active, managed_by_system)
VALUES
    (
        1,
        'financial',
        'Financial',
        'Records, requests, or confirmations of financial transactions or obligations.',
        NULL,
        NULL,
        TRUE,
        TRUE
    ),
    (
        1,
        'commercial-marketing',
        'Commercial & Marketing',
        'Influences purchasing or engagement decisions (includes legitimate newsletters and promotions).',
        NULL,
        NULL,
        TRUE,
        TRUE
    ),
    (
        1,
        'work-professional',
        'Work & Professional',
        'Related to employment, collaboration, or professional identity.',
        NULL,
        NULL,
        TRUE,
        TRUE
    ),
    (
        1,
        'personal-social',
        'Personal & Social',
        'Personal relationships, community, education, and non-billing healthcare communications.',
        NULL,
        NULL,
        TRUE,
        TRUE
    ),
    (
        1,
        'account-identity',
        'Account & Identity',
        'Manages access, identity, or account state (login alerts, password resets, confirmations).',
        NULL,
        NULL,
        TRUE,
        TRUE
    ),
    (
        1,
        'system-automated',
        'System & Automated',
        'Machine-generated state/event/failure notifications (GitHub, monitoring, CI/CD, SaaS).',
        NULL,
        NULL,
        TRUE,
        TRUE
    )
ON CONFLICT (slug) DO NOTHING;

-- Message->taxonomy assignment (source of truth for Gmail label sync).
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

-- Gmail label push outbox for incremental sync.
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

-- Retention archive outbox (two-phase: plan in DB, then long-running Gmail push).
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

CREATE INDEX IF NOT EXISTS idx_email_from_domain
    ON email_message(from_domain);

CREATE INDEX IF NOT EXISTS idx_email_unread
    ON email_message(is_unread);

CREATE INDEX IF NOT EXISTS idx_email_internal_date
    ON email_message(internal_date);
