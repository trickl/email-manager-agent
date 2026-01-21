"""Pipeline key/value state.

Used for:
- Incremental ingestion checkpoint (`last_ingested_internal_date`)
- Current phase tracking (`current_phase`)

This is intentionally simple: Postgres is our canonical source of truth.
"""

from __future__ import annotations

from datetime import datetime, timezone


KEY_LAST_INGESTED_INTERNAL_DATE = "last_ingested_internal_date"
KEY_CURRENT_PHASE = "current_phase"

# Tier-0 retention default. Stored as an integer number of days in pipeline_kv.
# If unset, backend behavior defaults to 2 years.
KEY_RETENTION_DEFAULT_DAYS = "retention_default_days"
DEFAULT_RETENTION_DEFAULT_DAYS = 365 * 2


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def get_kv(engine, key: str) -> str | None:
    from sqlalchemy import text

    query = text("SELECT value FROM pipeline_kv WHERE key = :key")
    with engine.begin() as conn:
        row = conn.execute(query, {"key": key}).fetchone()
    return None if row is None else str(row[0])


def set_kv(engine, key: str, value: str) -> None:
    from sqlalchemy import text

    query = text(
        """
        INSERT INTO pipeline_kv (key, value, updated_at)
        VALUES (:key, :value, :updated_at)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"key": key, "value": value, "updated_at": _now_utc()})


def get_checkpoint_internal_date(engine) -> datetime | None:
    raw = get_kv(engine, KEY_LAST_INGESTED_INTERNAL_DATE)
    if not raw:
        return None

    # ISO-8601 parsing: allow trailing Z.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return datetime.fromisoformat(raw)


def set_checkpoint_internal_date(engine, dt: datetime) -> None:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    set_kv(engine, KEY_LAST_INGESTED_INTERNAL_DATE, dt.isoformat())


def clear_checkpoint_internal_date(engine) -> None:
    """Clear the last-ingested checkpoint (forces a full ingest on next run)."""

    from sqlalchemy import text

    q = text("DELETE FROM pipeline_kv WHERE key = :key")
    with engine.begin() as conn:
        conn.execute(q, {"key": KEY_LAST_INGESTED_INTERNAL_DATE})


def set_current_phase(engine, phase: str) -> None:
    set_kv(engine, KEY_CURRENT_PHASE, phase)


def get_retention_default_days(engine) -> int:
    """Get the Tier-0 retention default (days).

    Returns:
        The configured default number of days, or 2 years (730) if not set.
    """

    raw = get_kv(engine, KEY_RETENTION_DEFAULT_DAYS)
    if raw is None or str(raw).strip() == "":
        return int(DEFAULT_RETENTION_DEFAULT_DAYS)

    try:
        n = int(str(raw).strip())
        if n <= 0:
            return int(DEFAULT_RETENTION_DEFAULT_DAYS)
        return int(n)
    except Exception:
        return int(DEFAULT_RETENTION_DEFAULT_DAYS)


def set_retention_default_days(engine, days: int) -> None:
    """Set the Tier-0 retention default (days)."""

    set_kv(engine, KEY_RETENTION_DEFAULT_DAYS, str(int(days)))
