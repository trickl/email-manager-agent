"""Backfill message->taxonomy assignments from existing email_message labels.

If the labeling pipeline previously persisted email_message.category/subcategory but failed to
create message_taxonomy_label rows (e.g. due to malformed Tier-2 strings), this script can
repair the taxonomy assignment table and enqueue Gmail sync outbox rows.

It only fills in *missing* assignments; it does not overwrite existing message_taxonomy_label.
"""

from __future__ import annotations

# Ensure the backend package root (email-intelligence/backend) is on sys.path so
# `import app.*` works when this script is executed from other directories.
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from sqlalchemy import text

from app.db.postgres import engine


def main() -> None:
    q = text(
        """
        WITH candidates AS (
            SELECT
                em.id AS message_id,
                COALESCE(t2.id, t1.id) AS taxonomy_label_id
            FROM email_message em
            LEFT JOIN taxonomy_label t1
                ON t1.level = 1 AND t1.name = em.category
            LEFT JOIN taxonomy_label p
                ON p.level = 1 AND p.name = em.category
            LEFT JOIN taxonomy_label t2
                ON t2.level = 2 AND t2.parent_id = p.id AND t2.name = em.subcategory
            WHERE em.category IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM message_taxonomy_label mtl
                  WHERE mtl.message_id = em.id
              )
        ),
        ins AS (
            INSERT INTO message_taxonomy_label (message_id, taxonomy_label_id, assigned_at, confidence)
            SELECT message_id, taxonomy_label_id, NOW(), NULL
            FROM candidates
            WHERE taxonomy_label_id IS NOT NULL
            ON CONFLICT (message_id, taxonomy_label_id)
            DO NOTHING
            RETURNING message_id
        )
        INSERT INTO label_push_outbox (message_id, reason)
        SELECT i.message_id, 'backfill_taxonomy'
        FROM ins i
        WHERE NOT EXISTS (
            SELECT 1
            FROM label_push_outbox o
            WHERE o.message_id = i.message_id AND o.processed_at IS NULL
        )
        """
    )

    with engine.begin() as conn:
        res = conn.execute(q)
        # res.rowcount is driver-dependent for multi-CTE insert; compute explicit counts.
        inserted = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM message_taxonomy_label
                """
            )
        ).scalar()

    print("backfill completed; total message_taxonomy_label rows:", int(inserted or 0))


if __name__ == "__main__":
    main()
