"""PostgreSQL connector stub.

Phase 0 requirement: create an engine and test the DB connection on import.
"""

from __future__ import annotations

import os

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "EMAIL_INTEL_DATABASE_URL",
    os.getenv(
        "DATABASE_URL",
        "postgresql://email:email@localhost:5432/email_intelligence",
    ),
)

engine = create_engine(DATABASE_URL)


def test_connection() -> None:
    """Verify the database is reachable."""

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


# Phase 0: test connection on import (fail fast if DB isn't reachable)
test_connection()
