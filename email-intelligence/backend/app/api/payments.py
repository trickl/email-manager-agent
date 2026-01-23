"""Payments API.

Provides UI-facing endpoints for payment analytics and recent payments.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

from app.api.models import PaymentsAnalyticsResponse, PaymentsListResponse
from app.db.postgres import engine
from app.repository.payment_metadata_repository import get_payment_analytics
from app.repository.payment_metadata_repository import get_primary_currency
from app.repository.payment_metadata_repository import list_recent_payments

router = APIRouter(prefix="/api/payments", tags=["payments"])


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@router.get("/recent", response_model=PaymentsListResponse)
def get_recent_payments(
    months: int = 3,
    limit: int = 200,
    currency: str | None = None,
) -> PaymentsListResponse:
    rows = list_recent_payments(engine=engine, months=months, limit=limit, currency=currency)
    return PaymentsListResponse(generated_at=_now_utc(), payments=rows)  # type: ignore[arg-type]


@router.get("/analytics", response_model=PaymentsAnalyticsResponse)
def get_payments_analytics(
    months: int = 6,
    currency: str | None = None,
) -> PaymentsAnalyticsResponse:
    primary_currency, currencies = get_primary_currency(engine=engine, months=months)
    selected_currency = currency or primary_currency

    analytics = get_payment_analytics(
        engine=engine,
        months=months,
        currency=selected_currency,
    )

    return PaymentsAnalyticsResponse(
        generated_at=_now_utc(),
        window_start=analytics["window_start"],
        window_end=analytics["window_end"],
        currency=selected_currency,
        available_currencies=currencies,
        payment_count=int(analytics["payment_count"]),
        total_spend=float(analytics["total_spend"]),
        by_vendor=analytics["by_vendor"],
        by_category=analytics["by_category"],
        by_recurring=analytics["by_recurring"],
        by_frequency=analytics["by_frequency"],
        by_month=analytics["by_month"],
    )
