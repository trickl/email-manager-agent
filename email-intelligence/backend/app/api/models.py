"""API models for the Email Intelligence backend."""

from __future__ import annotations

from datetime import date, datetime, time

from pydantic import BaseModel


class DashboardNode(BaseModel):
    id: str
    name: str
    count: int
    unread_count: int
    unread_ratio: float
    frequency: str | None = None
    children: list["DashboardNode"]


class DashboardTreeResponse(BaseModel):
    generated_at: datetime
    root: DashboardNode


class JobProgress(BaseModel):
    total: int | None = None
    processed: int
    percent: float | None = None


class JobCounters(BaseModel):
    inserted: int = 0
    skipped_existing: int = 0
    failed: int = 0


class JobStatusResponse(BaseModel):
    job_id: str
    type: str
    state: str
    phase: str | None = None
    started_at: datetime
    updated_at: datetime
    progress: JobProgress
    counters: JobCounters
    message: str | None = None
    # Best-effort error detail for jobs that may partially succeed.
    # Kept intentionally small (sampled) to avoid returning huge payloads.
    error_samples: list[str] | None = None
    eta_hint: str | None = None


class CurrentJobResponse(BaseModel):
    active: dict | None = None


class EmailMessageSummary(BaseModel):
    gmail_message_id: str
    subject: str | None = None
    from_domain: str
    internal_date: datetime
    is_unread: bool
    category: str | None = None
    subcategory: str | None = None
    label_ids: list[str] = []
    label_names: list[str] = []


class MessageSamplesResponse(BaseModel):
    node_id: str
    generated_at: datetime
    messages: list[EmailMessageSummary]


class FutureEventItem(BaseModel):
    message_id: int

    event_date: date
    start_time: time | None = None
    end_time: time | None = None
    end_time_inferred: bool = False
    timezone: str | None = None
    event_type: str | None = None
    event_name: str | None = None

    calendar_event_id: str | None = None
    calendar_checked_at: datetime | None = None
    calendar_published_at: datetime | None = None
    hidden_at: datetime | None = None

    # Email context
    subject: str | None = None
    from_domain: str
    internal_date: datetime


class FutureEventsResponse(BaseModel):
    generated_at: datetime
    events: list[FutureEventItem]


class HideEventResponse(BaseModel):
    message_id: int
    hidden: bool
    hidden_at: datetime | None = None


class CalendarCheckResponse(BaseModel):
    message_id: int
    calendar_ical_uid: str
    exists: bool
    calendar_event_id: str | None = None
    calendar_checked_at: datetime


class CalendarPublishResponse(BaseModel):
    message_id: int
    calendar_ical_uid: str
    already_existed: bool
    calendar_event_id: str
    calendar_published_at: datetime | None = None


class PaymentItem(BaseModel):
    message_id: int

    item_name: str | None = None
    vendor_name: str | None = None
    item_category: str | None = None
    cost_amount: float | None = None
    cost_currency: str | None = None
    is_recurring: bool | None = None
    frequency: str | None = None
    payment_date: date | None = None
    payment_fingerprint: str | None = None

    subject: str | None = None
    from_domain: str | None = None
    internal_date: datetime | None = None


class PaymentsListResponse(BaseModel):
    generated_at: datetime
    payments: list[PaymentItem]


class SpendByVendor(BaseModel):
    vendor: str
    total_spend: float


class SpendByCategory(BaseModel):
    category: str
    total_spend: float


class SpendByRecurring(BaseModel):
    kind: str
    payment_count: int
    total_spend: float


class SpendByFrequency(BaseModel):
    frequency: str
    payment_count: int
    total_spend: float


class SpendByMonth(BaseModel):
    month: date
    total_spend: float
    payment_count: int


class PaymentsAnalyticsResponse(BaseModel):
    generated_at: datetime
    window_start: date
    window_end: date
    currency: str | None = None
    available_currencies: list[str] = []
    payment_count: int
    total_spend: float
    by_vendor: list[SpendByVendor]
    by_category: list[SpendByCategory]
    by_recurring: list[SpendByRecurring]
    by_frequency: list[SpendByFrequency]
    by_month: list[SpendByMonth]
    
