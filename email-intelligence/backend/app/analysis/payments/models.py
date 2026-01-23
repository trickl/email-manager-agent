"""Models for payment extraction."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, Field


class PaymentExtraction(BaseModel):
    """Structured payment data extracted from an email."""

    item_name: str | None = Field(default=None, description="Purchased item or service name")
    vendor_name: str | None = Field(default=None, description="Supplier/vendor name")
    item_category: str | None = Field(
        default=None,
        description="Spending category: Food|Entertainment|Technology|Lifestyle|Domestic Bills|Other",
    )

    cost_amount: str | float | None = Field(
        default=None,
        description="Payment amount as a number (string or float accepted)",
    )
    cost_currency: str | None = Field(
        default=None,
        description="ISO-4217 currency code, e.g. GBP, USD, EUR",
    )

    is_recurring: bool | None = Field(default=None, description="True if recurring payment")
    frequency: str | None = Field(
        default=None,
        description="Recurring frequency, e.g. daily|weekly|biweekly|monthly|quarterly|yearly",
    )

    payment_date: str | None = Field(
        default=None,
        description="Payment date as YYYY-MM-DD",
    )

    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    notes: str | None = Field(default=None, description="Optional brief note about ambiguity")


class NormalizedPaymentExtraction(BaseModel):
    """Normalized representation suitable for persistence."""

    item_name: str | None
    vendor_name: str | None
    item_category: str | None

    cost_amount: Decimal | None
    cost_currency: str | None

    is_recurring: bool | None
    frequency: str | None

    payment_date: date | None

    payment_fingerprint: str | None

    confidence: float | None = Field(default=None, ge=0.0, le=1.0)

    raw_json: dict | None = None
    model: str | None = None
    prompt_version: str | None = None
    notes: str | None = None
