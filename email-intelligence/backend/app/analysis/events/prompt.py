"""Prompt contract for extracting event metadata from emails."""

from __future__ import annotations


PROMPT_VERSION = "event-extract-v2"


def build_event_extraction_prompt(
    *,
    subject: str | None,
    from_domain: str | None,
    internal_date_iso: str | None,
    body: str,
) -> str:
    """Build a prompt that requests strict JSON output.

    We intentionally ask for a small schema to keep parsing reliable.

    Args:
        subject: Email subject (may be None).
        from_domain: Sender domain.
        internal_date_iso: Email received timestamp (ISO-8601, UTC) for hints.
        body: Extracted email body text (best-effort).

    Returns:
        Prompt string.
    """

    subj = (subject or "").strip()
    dom = (from_domain or "").strip()

    # Keep body bounded; upstream caller should already trim, but belt + braces.
    body = (body or "").strip()
    if len(body) > 20_000:
        body = body[:20_000]

    return (
        "You are an assistant that extracts calendar event details from emails.\n"
        "Your job is to identify whether this email contains details for a single event (tickets, bookings, reservations, appointments).\n\n"
        "Return ONLY valid JSON. No markdown. No code fences. No commentary.\n"
        "If the email does not describe an event, return JSON with event_name/event_date/start_time/end_time all null and confidence <= 0.2.\n\n"
        "Extract these fields:\n"
        "- event_name: string|null (a concise human title)\n"
        "- event_date: string|null in ISO date format YYYY-MM-DD\n"
        "- start_time: string|null in 24h time HH:MM\n"
        "- end_time: string|null in 24h time HH:MM (if unknown, set null)\n"
        "- timezone: string|null (prefer IANA name like 'Europe/London'; else offset like '+01:00')\n"
        "- event_type: string|null chosen from {Theatre, Comedy, Opera, Ballet, Cinema, Social, Other}\n"
        "- confidence: number 0..1\n"
        "- notes: string|null (brief, only if ambiguous)\n\n"
        "Rules:\n"
        "- Use only information supported by the email content.\n"
        "- Do not invent an end_time. If not present, set end_time to null (it may be inferred later by the system).\n"
        "- If you choose an event_type, it MUST be exactly one of the allowed values (case-sensitive).\n"
        "- If multiple events are present, pick the most prominent one and mention that in notes.\n\n"
        f"Context hints (may be missing): subject={subj!r}, from_domain={dom!r}, received_at={internal_date_iso!r}.\n\n"
        "Email body:\n"
        "---\n"
        f"{body}\n"
        "---\n"
    )
