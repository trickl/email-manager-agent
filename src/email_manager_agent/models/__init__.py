"""Data models for Email Manager Agent.

This module contains Pydantic models for data validation and serialization.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EmailCategory(str, Enum):
    """Email category enumeration."""

    PERSONAL = "personal"
    WORK = "work"
    NEWSLETTER = "newsletter"
    PROMOTIONAL = "promotional"
    SPAM = "spam"
    SOCIAL = "social"
    UPDATES = "updates"
    FORUMS = "forums"


class ImportanceLevel(str, Enum):
    """Email importance level enumeration."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class EmailMessage(BaseModel):
    """Email message model."""

    id: str = Field(description="Unique message ID")
    thread_id: str = Field(description="Thread ID")
    subject: str = Field(description="Email subject")
    sender: str = Field(description="Sender email address")
    recipient: str = Field(description="Recipient email address")
    date: datetime = Field(description="Email date")
    body: str = Field(description="Email body content")
    labels: list[str] = Field(default_factory=list, description="Gmail labels")


class EmailCategorization(BaseModel):
    """Email categorization result."""

    category: EmailCategory = Field(description="Assigned category")
    importance: ImportanceLevel = Field(description="Importance level")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score")
    reasoning: str = Field(description="Explanation for categorization")
    suggested_actions: list[str] = Field(
        default_factory=list,
        description="Suggested actions for the email",
    )
    is_subscription: bool = Field(
        default=False,
        description="Whether this is a subscription email",
    )
    unsubscribe_link: Optional[str] = Field(
        default=None,
        description="Unsubscribe link if available",
    )


class ProcessingResult(BaseModel):
    """Result of processing an email."""

    email_id: str = Field(description="Processed email ID")
    categorization: EmailCategorization = Field(description="Categorization result")
    processed_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Processing timestamp",
    )
    processing_time_ms: float = Field(description="Processing time in milliseconds")
    success: bool = Field(default=True, description="Whether processing succeeded")
    error: Optional[str] = Field(default=None, description="Error message if failed")
