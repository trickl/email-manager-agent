"""Cluster analysis helpers (frequency and unread ratio labels)."""

from __future__ import annotations

from datetime import datetime


def frequency_label(dates: list[datetime]) -> str:
    """Compute an approximate frequency label from internal_date timestamps."""

    if len(dates) < 2:
        return "yearly"

    ordered = sorted(dates)
    deltas = []
    for a, b in zip(ordered, ordered[1:], strict=False):
        deltas.append((b - a).total_seconds())

    avg = sum(deltas) / len(deltas)

    day = 24 * 3600
    if avg <= 2 * day:
        return "daily"
    if avg <= 10 * day:
        return "weekly"
    if avg <= 40 * day:
        return "monthly"
    if avg <= 150 * day:
        return "quarterly"
    return "yearly"


def unread_ratio_label(is_unread_flags: list[bool]) -> str:
    if not is_unread_flags:
        return "none"

    total = len(is_unread_flags)
    unread_count = sum(1 for x in is_unread_flags if x)
    ratio = unread_count / total

    if ratio == 1.0:
        return "all"
    if ratio >= 0.9:
        return "almost all"
    if ratio == 0.0:
        return "none"
    if ratio <= 0.1:
        return "almost none"
    return "some"
