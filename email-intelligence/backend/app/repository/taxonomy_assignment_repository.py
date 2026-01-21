"""Persist message→taxonomy assignments and enqueue Gmail sync events."""

from __future__ import annotations

from typing import Any


def upsert_message_taxonomy_assignment(
    *,
    engine: Any,
    gmail_message_id: str,
    category: str,
    subcategory: str | None,
    confidence: float | None = None,
) -> None:
    """Set a message's taxonomy assignment and enqueue an outbox event.

    The system currently assumes each message has at most one taxonomy assignment
    (Tier-2 when present, otherwise Tier-1).

    Args:
        engine: SQLAlchemy engine.
        gmail_message_id: Gmail message id.
        category: Tier-1 category name.
        subcategory: Tier-2 subcategory name (optional).
        confidence: Optional confidence.
    """

    from sqlalchemy import text

    with engine.begin() as conn:
        msg = conn.execute(
            text("SELECT id FROM email_message WHERE gmail_message_id = :gid"),
            {"gid": str(gmail_message_id)},
        ).first()
        if not msg:
            return
        message_id = int(msg[0])

        # Find taxonomy label id (prefer Tier-2 when present, otherwise Tier-1).
        tid = None
        if subcategory:
            tid = conn.execute(
                text(
                    """
                    SELECT c.id
                    FROM taxonomy_label c
                    JOIN taxonomy_label p ON p.id = c.parent_id
                    WHERE c.level = 2 AND p.level = 1 AND p.name = :cat AND c.name = :sub
                    """
                ),
                {"cat": str(category), "sub": str(subcategory)},
            ).scalar()

        # If Tier-2 lookup fails (e.g., malformed/overlong model output), fall back to Tier-1.
        if tid is None:
            tid = conn.execute(
                text(
                    """
                    SELECT id
                    FROM taxonomy_label
                    WHERE level = 1 AND name = :cat
                    """
                ),
                {"cat": str(category)},
            ).scalar()

        if tid is None:
            return
        taxonomy_label_id = int(tid)

        # Enforce a single assignment per message by replacing existing rows.
        conn.execute(
            text("DELETE FROM message_taxonomy_label WHERE message_id = :mid"),
            {"mid": message_id},
        )

        conn.execute(
            text(
                """
                INSERT INTO message_taxonomy_label (message_id, taxonomy_label_id, assigned_at, confidence)
                VALUES (:mid, :tid, NOW(), :conf)
                ON CONFLICT (message_id, taxonomy_label_id)
                DO UPDATE SET assigned_at = EXCLUDED.assigned_at, confidence = EXCLUDED.confidence
                """
            ),
            {"mid": message_id, "tid": taxonomy_label_id, "conf": confidence},
        )

        # Enqueue an outbox row if there isn't already a pending one.
        conn.execute(
            text(
                """
                INSERT INTO label_push_outbox (message_id, reason)
                SELECT :mid, 'label_assigned'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM label_push_outbox
                    WHERE message_id = :mid AND processed_at IS NULL
                )
                """
            ),
            {"mid": message_id},
        )
