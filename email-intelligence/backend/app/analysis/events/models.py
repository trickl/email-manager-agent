"""Models for event extraction.

We keep the LLM response contract intentionally small and tolerant:
- The model may omit unknown fields.
- We separately validate/normalize before writing to Postgres.
"""

from __future__ import annotations

from datetime import date, time

from pydantic import BaseModel, Field


class EventExtraction(BaseModel):
    """Structured event data extracted from an email."""

    event_name: str | None = Field(default=None, description="Event name/title")

    # Prefer ISO-8601 date and 24h times.
    event_date: str | None = Field(default=None, description="Event date as YYYY-MM-DD")
    start_time: str | None = Field(default=None, description="Start time as HH:MM (24h)")
    end_time: str | None = Field(default=None, description="End time as HH:MM (24h)")

    timezone: str | None = Field(
        default=None,
        description="Timezone as IANA name (preferred) or fixed offset like +01:00",
    )

    event_type: str | None = Field(
        default=None,
        description=(
            "Optional coarse type used for end-time inference. "
            "Examples: Theatre|Comedy|Opera|Ballet|Cinema|Social|Other"
        ),
    )

    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    notes: str | None = Field(default=None, description="Optional brief note about ambiguity")


class NormalizedEventExtraction(BaseModel):
    """Normalized representation suitable for persistence."""

    event_name: str | None
    event_type: str | None

    event_date: date | None
    start_time: time | None
    end_time: time | None
    timezone: str | None

    end_time_inferred: bool = False
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)

    raw_json: dict | None = None
    model: str | None = None
    prompt_version: str | None = None
    notes: str | None = None
