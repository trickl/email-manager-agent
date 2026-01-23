"""Heuristics for event extraction.

We only use these when an email doesn't provide an explicit end time.
The goal is a *best guess* that is clearly marked as inferred.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta


DEFAULT_DURATION_MINUTES_BY_TYPE: dict[str, int] = {
    # Many theatre tickets include doors/opening time but not end.
    "theatre": 150,
    "comedy": 120,
    "opera": 210,
    "ballet": 180,
    "cinema": 130,
    "social": 120,
    "other": 120,
}


def infer_end_time(
    *,
    event_type: str | None,
    event_date: date | None,
    start_time: time | None,
) -> time | None:
    """Infer an end time from a start time and coarse event type.

    Args:
        event_type: Coarse type label. If unknown, we fall back to "other".
        event_date: Event date (required to do arithmetic safely).
        start_time: Start time.

    Returns:
        A best-guess end time, or None if we can't infer.
    """

    if event_date is None or start_time is None:
        return None

    et = (event_type or "").strip().lower() or "other"
    minutes = DEFAULT_DURATION_MINUTES_BY_TYPE.get(et, DEFAULT_DURATION_MINUTES_BY_TYPE["other"])

    start_dt = datetime.combine(event_date, start_time)
    end_dt = start_dt + timedelta(minutes=int(minutes))
    return end_dt.time()
