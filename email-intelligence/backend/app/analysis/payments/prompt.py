"""Prompt contract for extracting payment metadata from emails."""

from __future__ import annotations

PROMPT_VERSION = "payment-extract-v1"


def build_payment_extraction_prompt(
    *,
    subject: str | None,
    from_domain: str | None,
    internal_date_iso: str | None,
    body: str,
) -> str:
    """Build a prompt that requests strict JSON output.

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

    body = (body or "").strip()
    if len(body) > 20_000:
        body = body[:20_000]

    return (
        "You are an assistant that extracts payment details from emails.\n"
        "Your job is to identify whether this email contains a payment or charge (receipts, invoices, renewals).\n\n"
        "Return ONLY valid JSON. No markdown. No code fences. No commentary.\n"
        "If the email does not describe a payment, return JSON with item_name/vendor_name/cost_amount/cost_currency/payment_date all null and confidence <= 0.2.\n\n"
        "Extract these fields:\n"
        "- item_name: string|null (concise name of the purchased item or service)\n"
        "- vendor_name: string|null (merchant or supplier)\n"
        "- item_category: string|null chosen from {Food, Entertainment, Technology, Lifestyle, Domestic Bills, Other}\n"
        "- cost_amount: number|string|null (numeric amount, no currency symbols preferred)\n"
        "- cost_currency: string|null (ISO-4217 like GBP, USD, EUR)\n"
        "- is_recurring: boolean|null (true if recurring, false if one-off)\n"
        "- frequency: string|null chosen from {daily, weekly, biweekly, monthly, quarterly, yearly}\n"
        "- payment_date: string|null in ISO date format YYYY-MM-DD\n"
        "- confidence: number 0..1\n"
        "- notes: string|null (brief, only if ambiguous)\n\n"
        "Rules:\n"
        "- Use only information supported by the email content.\n"
        "- If you choose a category, it MUST be exactly one of the allowed values (case-sensitive).\n"
        "- If recurring, set is_recurring=true and include frequency.\n"
        "- If one-off, set is_recurring=false and frequency=null.\n"
        "- If you choose a frequency, it MUST be exactly one of the allowed values (lowercase).\n"
        "- If multiple payments are present, pick the most prominent one and mention that in notes.\n\n"
        f"Context hints (may be missing): subject={subj!r}, from_domain={dom!r}, received_at={internal_date_iso!r}.\n\n"
        "Email body:\n"
        "---\n"
        f"{body}\n"
        "---\n"
    )
