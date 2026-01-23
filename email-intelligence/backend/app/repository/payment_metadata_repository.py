"""Repository helpers for payment extraction metadata."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import json


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def list_messages_in_category_any_subcategory(
    *,
    engine: Any,
    category: str,
    limit: int | None = 500,
) -> list[dict[str, Any]]:
    """List candidate messages for payment extraction by category.

    Args:
        engine: SQLAlchemy engine.
        category: Tier-1 category.
        limit: Max rows.

    Returns:
        Rows with message_id, gmail_message_id, subject, from_domain, internal_date.
    """

    from sqlalchemy import text

    where = [
        "em.category = :category",
        "NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))",
    ]
    params: dict[str, object] = {"category": str(category)}

    where_sql = " AND ".join(where)

    limit_sql = ""
    if limit is not None:
        safe_limit = max(1, min(int(limit), 200_000))
        params["limit"] = safe_limit
        limit_sql = "LIMIT :limit"

    q = text(
        f"""
        SELECT
            em.id AS message_id,
            em.gmail_message_id AS gmail_message_id,
            em.subject AS subject,
            em.from_domain AS from_domain,
            em.internal_date AS internal_date
        FROM email_message em
        WHERE {where_sql}
        ORDER BY em.internal_date ASC, em.id ASC
        {limit_sql}
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, params).mappings().all()

    return [dict(r) for r in rows]


def list_messages_received_since(
    *,
    engine: Any,
    received_since: datetime,
    limit: int | None = 5000,
    include_trash: bool = False,
) -> list[dict[str, Any]]:
    """List candidate messages received since a given timestamp."""

    from sqlalchemy import text

    where = [
        "em.internal_date >= :received_since",
        "em.gmail_message_id NOT LIKE 'fake-%'",
    ]
    if not include_trash:
        where.append("NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))")

    where_sql = " AND ".join(where)

    params: dict[str, object] = {"received_since": received_since}
    limit_sql = ""
    if limit is not None:
        safe_limit = max(1, min(int(limit), 200_000))
        params["limit"] = safe_limit
        limit_sql = "LIMIT :limit"

    q = text(
        f"""
        SELECT
            em.id AS message_id,
            em.gmail_message_id AS gmail_message_id,
            em.subject AS subject,
            em.from_domain AS from_domain,
            em.internal_date AS internal_date
        FROM email_message em
        WHERE {where_sql}
        ORDER BY em.internal_date ASC, em.id ASC
        {limit_sql}
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, params).mappings().all()

    return [dict(r) for r in rows]


def upsert_message_payment_metadata(
    *,
    engine: Any,
    message_id: int,
    status: str,
    item_name: str | None,
    vendor_name: str | None,
    item_category: str | None,
    cost_amount: Decimal | None,
    cost_currency: str | None,
    is_recurring: bool | None,
    frequency: str | None,
    payment_date: date | None,
    payment_fingerprint: str | None,
    confidence: float | None,
    model: str | None,
    prompt_version: str | None,
    raw_json: dict | None,
    extracted_at: datetime | None = None,
    error: str | None = None,
) -> bool:
    """Insert or update message_payment_metadata.

    Returns:
        True if inserted, False if updated.
    """

    from sqlalchemy import text

    if extracted_at is None:
        extracted_at = _now_utc()

    q = text(
        """
        INSERT INTO message_payment_metadata (
            message_id,
            status,
            error,
            item_name,
            vendor_name,
            item_category,
            cost_amount,
            cost_currency,
            is_recurring,
            frequency,
            payment_date,
            payment_fingerprint,
            confidence,
            model,
            prompt_version,
            raw_json,
            extracted_at,
            updated_at
        )
        VALUES (
            :message_id,
            :status,
            :error,
            :item_name,
            :vendor_name,
            :item_category,
            :cost_amount,
            :cost_currency,
            :is_recurring,
            :frequency,
            :payment_date,
            :payment_fingerprint,
            :confidence,
            :model,
            :prompt_version,
            CAST(:raw_json AS JSONB),
            :extracted_at,
            NOW()
        )
        ON CONFLICT (message_id) DO UPDATE
        SET
            status = EXCLUDED.status,
            error = EXCLUDED.error,
            item_name = EXCLUDED.item_name,
            vendor_name = EXCLUDED.vendor_name,
            item_category = EXCLUDED.item_category,
            cost_amount = EXCLUDED.cost_amount,
            cost_currency = EXCLUDED.cost_currency,
            is_recurring = EXCLUDED.is_recurring,
            frequency = EXCLUDED.frequency,
            payment_date = EXCLUDED.payment_date,
            payment_fingerprint = EXCLUDED.payment_fingerprint,
            confidence = EXCLUDED.confidence,
            model = EXCLUDED.model,
            prompt_version = EXCLUDED.prompt_version,
            raw_json = EXCLUDED.raw_json,
            extracted_at = EXCLUDED.extracted_at,
            updated_at = NOW()
        RETURNING (xmax = 0) AS inserted
        """
    )

    payload = {
        "message_id": int(message_id),
        "status": str(status),
        "error": error,
        "item_name": item_name,
        "vendor_name": vendor_name,
        "item_category": item_category,
        "cost_amount": cost_amount,
        "cost_currency": cost_currency,
        "is_recurring": is_recurring,
        "frequency": frequency,
        "payment_date": payment_date,
        "payment_fingerprint": payment_fingerprint,
        "confidence": confidence,
        "model": model,
        "prompt_version": prompt_version,
        "raw_json": None if raw_json is None else json.dumps(raw_json),
        "extracted_at": extracted_at,
    }

    with engine.begin() as conn:
        row = conn.execute(q, payload).mappings().first()

    return bool(row["inserted"]) if row else False


def _window_dates(months: int) -> tuple[date, date]:
    months = max(1, int(months))
    end_date = date.today()
    start_date = end_date - timedelta(days=months * 30)
    return start_date, end_date


def list_recent_payments(
    *,
    engine: Any,
    months: int = 3,
    limit: int = 250,
    currency: str | None = None,
) -> list[dict[str, Any]]:
    """List recent outgoing payments (deduplicated by fingerprint)."""

    from sqlalchemy import text

    limit = max(1, min(int(limit), 2000))
    window_start, window_end = _window_dates(months)

    where = [
        "mem.status = 'succeeded'",
        "mem.cost_amount IS NOT NULL",
        "mem.payment_date IS NOT NULL",
        "mem.payment_date BETWEEN :window_start AND :window_end",
        "em.gmail_message_id NOT LIKE 'fake-%'",
        "NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))",
    ]

    params: dict[str, object] = {
        "window_start": window_start,
        "window_end": window_end,
        "limit": limit,
    }

    if currency:
        where.append("mem.cost_currency = :currency")
        params["currency"] = str(currency)

    where_sql = " AND ".join(where)

    q = text(
        f"""
        WITH deduped AS (
            SELECT DISTINCT ON (COALESCE(mem.payment_fingerprint, 'message-' || mem.message_id))
                mem.message_id,
                mem.item_name,
                mem.vendor_name,
                mem.item_category,
                mem.cost_amount,
                mem.cost_currency,
                mem.is_recurring,
                mem.frequency,
                mem.payment_date,
                mem.payment_fingerprint,
                em.subject,
                em.from_domain,
                em.internal_date
            FROM message_payment_metadata mem
            JOIN email_message em ON em.id = mem.message_id
            WHERE {where_sql}
            ORDER BY COALESCE(mem.payment_fingerprint, 'message-' || mem.message_id),
                     mem.payment_date DESC,
                     mem.message_id DESC
        )
        SELECT *
        FROM deduped
        ORDER BY payment_date DESC, message_id DESC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, params).mappings().all()

    return [dict(r) for r in rows]


def get_primary_currency(
    *,
    engine: Any,
    months: int = 6,
) -> tuple[str | None, list[str]]:
    """Return primary currency (by total spend) and all seen currencies."""

    from sqlalchemy import text

    window_start, window_end = _window_dates(months)

    q = text(
        """
        WITH deduped AS (
            SELECT DISTINCT ON (COALESCE(mem.payment_fingerprint, 'message-' || mem.message_id))
                mem.cost_amount,
                mem.cost_currency
            FROM message_payment_metadata mem
            JOIN email_message em ON em.id = mem.message_id
            WHERE mem.status = 'succeeded'
              AND mem.cost_amount IS NOT NULL
              AND mem.cost_currency IS NOT NULL
              AND mem.payment_date IS NOT NULL
              AND mem.payment_date BETWEEN :window_start AND :window_end
              AND em.gmail_message_id NOT LIKE 'fake-%'
              AND NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))
            ORDER BY COALESCE(mem.payment_fingerprint, 'message-' || mem.message_id),
                     mem.payment_date DESC,
                     mem.message_id DESC
        )
        SELECT cost_currency, SUM(cost_amount) AS total
        FROM deduped
        GROUP BY cost_currency
        ORDER BY total DESC
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"window_start": window_start, "window_end": window_end}).mappings().all()

    currencies = [str(r["cost_currency"]) for r in rows if r.get("cost_currency")]
    primary = currencies[0] if currencies else None
    return primary, currencies


def get_payment_analytics(
    *,
    engine: Any,
    months: int = 6,
    currency: str | None = None,
) -> dict[str, Any]:
    """Compute spend analytics for a recent window (deduplicated)."""

    from sqlalchemy import text

    window_start, window_end = _window_dates(months)

    params: dict[str, object] = {
        "window_start": window_start,
        "window_end": window_end,
    }

    currency_filter = ""
    if currency:
        currency_filter = "AND mem.cost_currency = :currency"
        params["currency"] = str(currency)

    deduped_cte = f"""
        WITH deduped AS (
            SELECT DISTINCT ON (COALESCE(mem.payment_fingerprint, 'message-' || mem.message_id))
                mem.message_id,
                mem.item_name,
                mem.vendor_name,
                mem.item_category,
                mem.cost_amount,
                mem.cost_currency,
                mem.is_recurring,
                mem.frequency,
                mem.payment_date
            FROM message_payment_metadata mem
            JOIN email_message em ON em.id = mem.message_id
            WHERE mem.status = 'succeeded'
              AND mem.cost_amount IS NOT NULL
              AND mem.payment_date IS NOT NULL
              AND mem.payment_date BETWEEN :window_start AND :window_end
              AND em.gmail_message_id NOT LIKE 'fake-%'
              AND NOT ('TRASH' = ANY(COALESCE(em.label_ids, ARRAY[]::text[])))
              {currency_filter}
            ORDER BY COALESCE(mem.payment_fingerprint, 'message-' || mem.message_id),
                     mem.payment_date DESC,
                     mem.message_id DESC
        )
    """

    totals_q = text(
        deduped_cte
        + """
        SELECT
            COUNT(*) AS payment_count,
            COALESCE(SUM(cost_amount), 0) AS total_spend
        FROM deduped
        """
    )

    vendor_q = text(
        deduped_cte
        + """
        SELECT
            COALESCE(NULLIF(vendor_name, ''), 'Unknown') AS vendor,
            COALESCE(SUM(cost_amount), 0) AS total_spend
        FROM deduped
        GROUP BY 1
        ORDER BY total_spend DESC
        LIMIT 20
        """
    )

    category_q = text(
        deduped_cte
        + """
        SELECT
            COALESCE(NULLIF(item_category, ''), 'Other') AS category,
            COALESCE(SUM(cost_amount), 0) AS total_spend
        FROM deduped
        GROUP BY 1
        ORDER BY total_spend DESC
        """
    )

    recurring_q = text(
        deduped_cte
        + """
        SELECT
            CASE WHEN COALESCE(is_recurring, false) THEN 'recurring' ELSE 'one_off' END AS kind,
            COUNT(*) AS payment_count,
            COALESCE(SUM(cost_amount), 0) AS total_spend
        FROM deduped
        GROUP BY 1
        ORDER BY total_spend DESC
        """
    )

    frequency_q = text(
        deduped_cte
        + """
        SELECT
            COALESCE(NULLIF(frequency, ''), 'unspecified') AS frequency,
            COUNT(*) AS payment_count,
            COALESCE(SUM(cost_amount), 0) AS total_spend
        FROM deduped
        WHERE COALESCE(is_recurring, false)
        GROUP BY 1
        ORDER BY total_spend DESC
        """
    )

    monthly_q = text(
        deduped_cte
        + """
        SELECT
            date_trunc('month', payment_date)::date AS month,
            COALESCE(SUM(cost_amount), 0) AS total_spend,
            COUNT(*) AS payment_count
        FROM deduped
        GROUP BY 1
        ORDER BY month ASC
        """
    )

    with engine.begin() as conn:
        totals = conn.execute(totals_q, params).mappings().first() or {}
        vendor_rows = conn.execute(vendor_q, params).mappings().all()
        category_rows = conn.execute(category_q, params).mappings().all()
        recurring_rows = conn.execute(recurring_q, params).mappings().all()
        frequency_rows = conn.execute(frequency_q, params).mappings().all()
        monthly_rows = conn.execute(monthly_q, params).mappings().all()

    return {
        "window_start": window_start,
        "window_end": window_end,
        "payment_count": int(totals.get("payment_count") or 0),
        "total_spend": float(totals.get("total_spend") or 0),
        "by_vendor": [dict(r) for r in vendor_rows],
        "by_category": [dict(r) for r in category_rows],
        "by_recurring": [dict(r) for r in recurring_rows],
        "by_frequency": [dict(r) for r in frequency_rows],
        "by_month": [dict(r) for r in monthly_rows],
    }
