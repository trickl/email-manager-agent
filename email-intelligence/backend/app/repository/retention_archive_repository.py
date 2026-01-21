"""Retention-driven archive planning and outbox processing.

This module implements the "two-phase" retention archive flow:
1) Plan: compute messages whose received time (email_message.internal_date) is older than the
    effective retention period and enqueue them into an outbox table in Postgres.
2) Push: a long-running job later reads that outbox and applies a Gmail marker label.

Design notes:
- The database is the source of truth for what should be labeled.
- The Gmail operation is intentionally separated so it can be run as a resumable job.
- The outbox is idempotent per message_id: re-planning resets failed/processed rows.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ArchiveOutboxRow:
    id: int
    message_id: int
    gmail_message_id: str


def plan_archive_outbox(*, engine, default_days: int) -> int:
    """Enqueue messages eligible for retention archive into the outbox.

    Args:
        engine: SQLAlchemy engine.
        default_days: Tier-0 default retention in days.

    Returns:
        Number of outbox rows inserted or reset.
    """

    from sqlalchemy import text

    # NOTE: Eligibility is based on when the email was received (em.internal_date).
    # This ensures retention reflects email age even if taxonomy labels were assigned recently.
    q = text(
        """
        WITH eligible AS (
            SELECT DISTINCT em.id AS message_id
            FROM email_message em
            JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
            JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
            LEFT JOIN taxonomy_label p ON p.id = tl.parent_id
            WHERE em.archived_at IS NULL
              AND em.gmail_message_id IS NOT NULL
                            AND em.internal_date <= (
                NOW() - (COALESCE(tl.retention_days, p.retention_days, :default_days)::text || ' days')::interval
              )
        )
        INSERT INTO archive_push_outbox (message_id, reason)
        SELECT e.message_id, 'retention_eligible'
        FROM eligible e
        ON CONFLICT (message_id)
        DO UPDATE SET
            created_at = NOW(),
            processed_at = NULL,
            error = NULL
        RETURNING message_id
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"default_days": int(default_days)}).fetchall()

    return int(len(rows))


def count_pending_outbox(*, engine) -> int:
    from sqlalchemy import text

    with engine.begin() as conn:
        n = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM archive_push_outbox o
                WHERE o.processed_at IS NULL
                """
            )
        ).scalar()

    return int(n or 0)


def fetch_pending_batch(*, engine, limit: int = 200) -> list[ArchiveOutboxRow]:
    """Fetch a batch of pending outbox rows.

    Returns rows that have not been processed yet.
    """

    from sqlalchemy import text

    q = text(
        """
        SELECT
            o.id AS outbox_id,
            o.message_id AS message_id,
            em.gmail_message_id AS gmail_message_id
        FROM archive_push_outbox o
        JOIN email_message em ON em.id = o.message_id
        WHERE o.processed_at IS NULL
          AND em.gmail_message_id IS NOT NULL
        ORDER BY o.id ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"limit": int(limit)}).mappings().all()

    return [
        ArchiveOutboxRow(
            id=int(r["outbox_id"]),
            message_id=int(r["message_id"]),
            gmail_message_id=str(r["gmail_message_id"]),
        )
        for r in rows
    ]


def mark_outbox_succeeded(*, engine, outbox_id: int, message_id: int) -> None:
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE archive_push_outbox
                SET processed_at = NOW(), error = NULL
                WHERE id = :id
                """
            ),
            {"id": int(outbox_id)},
        )
        conn.execute(
            text("UPDATE email_message SET archived_at = NOW() WHERE id = :id"),
            {"id": int(message_id)},
        )


def mark_outbox_failed(*, engine, outbox_id: int, error: str) -> None:
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE archive_push_outbox
                SET processed_at = NOW(), error = :error
                WHERE id = :id
                """
            ),
            {"id": int(outbox_id), "error": str(error)[:5000]},
        )
