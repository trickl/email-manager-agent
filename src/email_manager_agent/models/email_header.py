"""Header-only email metadata model.

This model intentionally excludes the full email body so that users can build a
local index with significantly reduced privacy risk compared to storing complete
messages.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class EmailHeader(BaseModel):
    """A minimal representation of an email message without the body."""

    gmail_id: str = Field(description="Gmail message ID")
    thread_id: str | None = Field(default=None, description="Gmail thread ID")
    internal_date_ms: int | None = Field(
        default=None, description="Internal timestamp in milliseconds since epoch"
    )

    subject: str = Field(default="", description="Subject header")

    # Keep both raw and parsed forms. Raw is useful for display (includes name).
    from_raw: str | None = Field(default=None, description="Raw From header")
    from_email: str | None = Field(default=None, description="Parsed sender email address")

    to_addrs: list[str] = Field(default_factory=list, description="Parsed To addresses")
    cc_addrs: list[str] = Field(default_factory=list, description="Parsed Cc addresses")
    bcc_addrs: list[str] = Field(default_factory=list, description="Parsed Bcc addresses")

    reply_to: str | None = Field(default=None, description="Reply-To header")
    date: datetime | None = Field(default=None, description="Parsed Date header")

    label_ids: list[str] = Field(default_factory=list, description="Gmail label IDs")

    is_unread: bool = Field(default=False, description="Whether message is unread")
    is_inbox: bool = Field(default=False, description="Whether message is in inbox")
    is_starred: bool = Field(default=False, description="Whether message is starred")
