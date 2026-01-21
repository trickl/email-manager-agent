"""API models for the Email Intelligence backend."""

from __future__ import annotations

from datetime import datetime

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
    
