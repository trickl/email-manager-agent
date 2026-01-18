"""Helpers for parsing Gmail message metadata into internal models."""

from __future__ import annotations

from datetime import datetime
from email.utils import getaddresses, parsedate_to_datetime
from typing import Any

from email_manager_agent.models import EmailHeader


def _header_map(message: dict[str, Any]) -> dict[str, str]:
    payload = message.get("payload") or {}
    headers = payload.get("headers") or []
    result: dict[str, str] = {}
    for h in headers:
        name = h.get("name")
        value = h.get("value")
        if isinstance(name, str) and isinstance(value, str):
            # Gmail can include duplicates; keep the first for now.
            result.setdefault(name.lower(), value)
    return result


def _parse_address_list(value: str | None) -> list[str]:
    if not value:
        return []
    # getaddresses returns list[(name, addr)]
    return [addr for _, addr in getaddresses([value]) if addr]


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None


def message_to_email_header(message: dict[str, Any]) -> EmailHeader:
    """Convert a Gmail API message (format=metadata) to EmailHeader.

    Args:
        message: Gmail API message dict.

    Returns:
        EmailHeader: Parsed header-only model.
    """

    hm = _header_map(message)

    label_ids = message.get("labelIds") or []
    if not isinstance(label_ids, list):
        label_ids = []

    from_raw = hm.get("from")
    from_addrs = _parse_address_list(from_raw)

    internal_date_ms: int | None
    internal_date_raw = message.get("internalDate")
    try:
        internal_date_ms = int(internal_date_raw) if internal_date_raw is not None else None
    except (TypeError, ValueError):
        internal_date_ms = None

    return EmailHeader(
        gmail_id=str(message.get("id") or ""),
        thread_id=str(message.get("threadId") or "") or None,
        internal_date_ms=internal_date_ms,
        subject=hm.get("subject") or "",
        from_raw=from_raw,
        from_email=from_addrs[0] if from_addrs else None,
        to_addrs=_parse_address_list(hm.get("to")),
        cc_addrs=_parse_address_list(hm.get("cc")),
        bcc_addrs=_parse_address_list(hm.get("bcc")),
        reply_to=hm.get("reply-to"),
        date=_parse_date(hm.get("date")),
        label_ids=[str(x) for x in label_ids if isinstance(x, str)],
        is_unread="UNREAD" in label_ids,
        is_inbox="INBOX" in label_ids,
        is_starred="STARRED" in label_ids,
    )
