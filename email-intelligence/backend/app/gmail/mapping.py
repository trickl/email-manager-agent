"""Mapping Gmail API metadata into the canonical domain model."""

from __future__ import annotations

from app.domain.email import EmailMessage
from app.gmail.client import GmailMessageMetadata
from app.utils.subject import normalize_subject


def _extract_domain(email_address: str) -> str:
    addr = (email_address or "").strip().lower()
    if "@" in addr:
        return addr.split("@", 1)[1]
    return addr


def metadata_to_domain(meta: GmailMessageMetadata) -> EmailMessage:
    """Map Gmail metadata into `EmailMessage`.

    Required transformations:
    - subject_normalized via normalize_subject
    - from_domain extracted from from_address
    - to/cc/bcc are lists of strings
    - internal_date is a parsed timestamp (UTC)
    """

    subject_norm = normalize_subject(meta.subject)

    return EmailMessage(
        gmail_message_id=meta.gmail_message_id,
        thread_id=meta.thread_id,
        subject=meta.subject,
        subject_normalized=subject_norm,
        from_address=meta.from_address,
        from_domain=_extract_domain(meta.from_address),
        to_addresses=meta.to_addresses,
        cc_addresses=meta.cc_addresses,
        bcc_addresses=meta.bcc_addresses,
        is_unread=meta.is_unread,
        internal_date=meta.internal_date,
    )
