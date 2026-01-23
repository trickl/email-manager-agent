"""Payment metadata extraction using Ollama.

This module is designed to be called in batch jobs.
It is best-effort: failures should be recorded per-message rather than aborting the whole job.
"""

from __future__ import annotations

import json
import re
import urllib.request
from datetime import date
from decimal import Decimal, InvalidOperation

from app.analysis.payments.models import NormalizedPaymentExtraction, PaymentExtraction
from app.analysis.payments.prompt import PROMPT_VERSION, build_payment_extraction_prompt


_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.S)


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _extract_json_object(raw: str) -> dict:
    """Extract the first JSON object from a raw model response."""

    raw = (raw or "").strip()
    if not raw:
        raise ValueError("empty model response")

    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    m = _JSON_OBJECT_RE.search(raw)
    if not m:
        raise ValueError("model response did not contain a JSON object")

    snippet = m.group(0)
    obj = json.loads(snippet)
    if not isinstance(obj, dict):
        raise ValueError("extracted JSON was not an object")
    return obj


_CURRENCY_SYMBOLS = {
    "£": "GBP",
    "€": "EUR",
    "$": "USD",
}


def _normalize_currency(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return raw.upper()


def _parse_amount(value: str | float | None) -> Decimal | None:
    if value is None:
        return None

    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None

    raw = str(value).strip()
    if not raw:
        return None

    # Remove currency symbols and thousands separators.
    for sym in _CURRENCY_SYMBOLS:
        raw = raw.replace(sym, "")

    # Detect decimal comma if no dot present.
    if "," in raw and "." not in raw:
        raw = raw.replace(",", ".")

    # Strip remaining grouping commas.
    raw = raw.replace(",", "")

    # Extract first numeric pattern.
    m = re.search(r"\d+(?:\.\d+)?", raw)
    if not m:
        return None

    try:
        return Decimal(m.group(0))
    except InvalidOperation:
        return None


def _parse_currency_from_text(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value)
    for sym, code in _CURRENCY_SYMBOLS.items():
        if sym in raw:
            return code

    m = re.search(r"\b([A-Za-z]{3})\b", raw)
    if not m:
        return None

    return m.group(1).upper()


_ALLOWED_FREQUENCIES = {
    "daily": "daily",
    "every day": "daily",
    "weekly": "weekly",
    "biweekly": "biweekly",
    "bi-weekly": "biweekly",
    "fortnightly": "biweekly",
    "monthly": "monthly",
    "quarterly": "quarterly",
    "yearly": "yearly",
    "annual": "yearly",
    "annually": "yearly",
}

_ALLOWED_CATEGORIES = {
    "food": "Food",
    "entertainment": "Entertainment",
    "technology": "Technology",
    "tech": "Technology",
    "lifestyle": "Lifestyle",
    "domestic bills": "Domestic Bills",
    "domestic": "Domestic Bills",
    "utilities": "Domestic Bills",
    "other": "Other",
}


def _normalize_frequency(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    key = raw.casefold()
    if key in _ALLOWED_FREQUENCIES:
        return _ALLOWED_FREQUENCIES[key]

    # Handle "every <period>" patterns.
    if key.startswith("every "):
        key = key.replace("every ", "", 1).strip()
        if key in _ALLOWED_FREQUENCIES:
            return _ALLOWED_FREQUENCIES[key]

    return None


def _normalize_name(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _normalize_vendor_key(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip().casefold()
    if not raw:
        return None
    return re.sub(r"[^a-z0-9]+", "", raw)


def _normalize_category(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    canonical = {"Food", "Entertainment", "Technology", "Lifestyle", "Domestic Bills", "Other"}
    if raw in canonical:
        return raw

    key = raw.casefold()
    if key in _ALLOWED_CATEGORIES:
        return _ALLOWED_CATEGORIES[key]

    return "Other"


def _compute_fingerprint(
    *,
    vendor_name: str | None,
    cost_amount: Decimal | None,
    cost_currency: str | None,
    payment_date: date | None,
) -> str | None:
    if not vendor_name or cost_amount is None or not cost_currency or payment_date is None:
        return None

    vendor_key = _normalize_vendor_key(vendor_name)
    if not vendor_key:
        return None

    amount_key = f"{cost_amount.quantize(Decimal('0.01')):.2f}"
    return f"{vendor_key}|{amount_key}|{cost_currency}|{payment_date.isoformat()}"


def _call_ollama_generate(*, host: str, model: str, prompt: str, timeout_seconds: int = 60) -> str:
    host = host.rstrip("/")

    payload = json.dumps({"model": model, "prompt": prompt, "stream": False}).encode("utf-8")
    req = urllib.request.Request(
        url=f"{host}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        data = json.loads(resp.read().decode("utf-8"))

    return (data.get("response") or "").strip()


def extract_payment_from_email(
    *,
    ollama_host: str,
    ollama_model: str,
    subject: str | None,
    from_domain: str | None,
    internal_date_iso: str | None,
    body: str,
) -> NormalizedPaymentExtraction:
    """Extract a single payment from an email body.

    Returns a normalized object ready for DB persistence.
    """

    prompt = build_payment_extraction_prompt(
        subject=subject,
        from_domain=from_domain,
        internal_date_iso=internal_date_iso,
        body=body,
    )

    raw = _call_ollama_generate(host=ollama_host, model=ollama_model, prompt=prompt)
    raw_obj = _extract_json_object(raw)

    parsed = PaymentExtraction.model_validate(raw_obj)

    item_name = _normalize_name(parsed.item_name)
    vendor_name = _normalize_name(parsed.vendor_name)
    item_category = _normalize_category(parsed.item_category)

    currency_hint = _normalize_currency(parsed.cost_currency)
    currency_from_amount = _parse_currency_from_text(
        parsed.cost_amount if parsed.cost_amount is not None else None
    )
    cost_currency = currency_hint or currency_from_amount

    cost_amount = _parse_amount(parsed.cost_amount)

    payment_date = _parse_iso_date(parsed.payment_date)

    frequency = _normalize_frequency(parsed.frequency)
    is_recurring = parsed.is_recurring
    if is_recurring is None and frequency is not None:
        is_recurring = True

    if is_recurring is False:
        frequency = None

    fingerprint = _compute_fingerprint(
        vendor_name=vendor_name,
        cost_amount=cost_amount,
        cost_currency=cost_currency,
        payment_date=payment_date,
    )

    return NormalizedPaymentExtraction(
        item_name=item_name,
        vendor_name=vendor_name,
        item_category=item_category,
        cost_amount=cost_amount,
        cost_currency=cost_currency,
        is_recurring=is_recurring,
        frequency=frequency,
        payment_date=payment_date,
        payment_fingerprint=fingerprint,
        confidence=parsed.confidence,
        raw_json=raw_obj,
        model=ollama_model,
        prompt_version=PROMPT_VERSION,
        notes=_normalize_name(parsed.notes),
    )
