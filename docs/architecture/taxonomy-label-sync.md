# Taxonomy-driven Gmail label sync (design)

## Goals

- Replace the removed policy/rules engine with a **taxonomy-first** system.
- Treat Postgres as the **source of truth** for:
  - taxonomy labels (including retention metadata)
  - message→taxonomy assignments
  - incremental sync “outbox” events
- Keep Gmail actions **non-destructive**:
  - apply taxonomy labels
  - retention sweep **archives** by removing `INBOX` (no deletes)

## Non-goals

- No generic policy engine, workflow engine, or rule language.
- No automatic deletes.

## Data model (Postgres)

### `taxonomy_label`
Stores label hierarchy and Gmail mapping.

Key fields:
- `level`, `parent_id`, `name`, `slug`, `description`
- `retention_days` (nullable): if set, drives retention-based archive eligibility
- `is_active`: inactive labels are not pushed to Gmail
- `gmail_label_id`: stored after existence-sync for fast message label updates
- `last_sync_at`, `sync_status`, `sync_error`: observability for label sync

### `message_taxonomy_label`
Join table: message assignments.

- `assigned_at` (timestamp) is the source of truth for retention aging.
- `confidence` is optional (for model outputs).

### `label_push_outbox`
Incremental sync queue.

- Rows are inserted when a message’s taxonomy assignment changes.
- A worker (or an API-triggered job) processes oldest-first and sets `processed_at`.

### `email_message.archived_at`
Set when retention sweep archives a message.

## Gmail label naming

Gmail label names are derived deterministically from taxonomy:

- Tier-1: `<Tier1 Name>`
- Tier-2: `<Tier1 Name>/<Tier2 Name>`

A marker label is also used for retention operations:

- `Archived`

The system stores Gmail label IDs in Postgres so message updates can avoid label-name lookups.

## API surface

### Taxonomy CRUD

- `GET /api/taxonomy` — list labels (includes derived `gmail_label_name` + sync fields)
- `POST /api/taxonomy` — create label
- `PUT /api/taxonomy/{id}` — update label fields
- `DELETE /api/taxonomy/{id}` — delete label (will fail if referenced by children due to FK)

### Gmail sync

- `POST /api/gmail-sync/labels/sync`
  - creates labels if missing
  - renames labels if taxonomy name changed
  - stores `gmail_label_id`

- `POST /api/gmail-sync/messages/push-bulk`
  - reads `message_taxonomy_label`
  - applies mapped Gmail label IDs to each message

- `POST /api/gmail-sync/messages/push-incremental`
  - processes `label_push_outbox`

### Retention (archive-only)

- `POST /api/gmail-sync/retention/preview`
  - counts and samples messages eligible for archive

- `POST /api/gmail-sync/retention/run`
  - **archives** eligible messages by removing `INBOX`
  - adds `Archived`
  - sets `email_message.archived_at`

## Retention rule

Option A (implemented): a message becomes eligible for archive when **any** assigned taxonomy label has:

- a non-null `retention_days`, and
- `assigned_at <= now - retention_days`

This is intentionally conservative: it avoids deleting messages and is reversible by removing the marker label and/or adding `INBOX` back.

## Observability & safety

- Label existence sync stores status (`ok` / `error`) and error text on each taxonomy label.
- Outbox processing records errors per row.
- Retention actions update only:
  - Gmail labels (`INBOX` removal + marker label add)
  - `email_message.archived_at`

## Next steps (expected evolution)

- Wire the labeling pipeline so that when a message is categorized, it:
  - writes to `message_taxonomy_label`
  - enqueues `label_push_outbox`
- Add idempotency keys + batched Gmail modifications.
- Add UI affordances for dry-runs and explicit confirmation before archiving.
