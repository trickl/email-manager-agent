"""Cleanup malformed category/subcategory strings persisted from LLM output.

This script is safe to re-run.

It targets known failure modes:
- "Tier-2 Subcategory: X" -> "X"
- "Subcategory: X" -> "X"
- "Note: ..." / "Notes: ..." -> NULL (we treat these as invalid meta output)

It applies to both:
- email_message.subcategory (dashboard/navigation)
- email_cluster.subcategory (cluster summaries)
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
    updates: list[tuple[str, str]] = [
        (
            "email_message tier2 prefix",
            """
            UPDATE email_message
            SET subcategory = btrim(regexp_replace(subcategory, '^\\s*tier\\s*[-\\s]*2\\s*subcategory\\s*:\\s*', '', 'i'))
            WHERE subcategory ~* '^\\s*tier\\s*[-\\s]*2\\s*subcategory\\s*:'
            """,
        ),
        (
            "email_message subcategory prefix",
            """
            UPDATE email_message
            SET subcategory = btrim(regexp_replace(subcategory, '^\\s*subcategory\\s*:\\s*', '', 'i'))
            WHERE subcategory ~* '^\\s*subcategory\\s*:'
            """,
        ),
        (
            "email_message note to null",
            """
            UPDATE email_message
            SET subcategory = NULL
            WHERE subcategory ~* '^\\s*notes?\\s*:'
               OR (lower(subcategory) LIKE '%chosen categories%' AND lower(subcategory) LIKE '%match%')
            """,
        ),
        (
            "email_cluster tier2 prefix",
            """
            UPDATE email_cluster
            SET subcategory = btrim(regexp_replace(subcategory, '^\\s*tier\\s*[-\\s]*2\\s*subcategory\\s*:\\s*', '', 'i'))
            WHERE subcategory ~* '^\\s*tier\\s*[-\\s]*2\\s*subcategory\\s*:'
            """,
        ),
        (
            "email_cluster subcategory prefix",
            """
            UPDATE email_cluster
            SET subcategory = btrim(regexp_replace(subcategory, '^\\s*subcategory\\s*:\\s*', '', 'i'))
            WHERE subcategory ~* '^\\s*subcategory\\s*:'
            """,
        ),
        (
            "email_cluster note to null",
            """
            UPDATE email_cluster
            SET subcategory = NULL
            WHERE subcategory ~* '^\\s*notes?\\s*:'
               OR (lower(subcategory) LIKE '%chosen categories%' AND lower(subcategory) LIKE '%match%')
            """,
        ),
    ]

    with engine.begin() as conn:
        for label, sql in updates:
            res = conn.execute(text(sql))
            print(f"{label}: updated {res.rowcount}")


if __name__ == "__main__":
    main()
