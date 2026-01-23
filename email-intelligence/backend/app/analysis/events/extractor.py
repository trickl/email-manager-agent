"""Event metadata extraction using Ollama.

This module is designed to be called in batch jobs.
It is best-effort: failures should be recorded per-message rather than aborting the whole job.
"""

from __future__ import annotations

import json
import logging
import re
import urllib.request
from datetime import date, time

from app.analysis.events.heuristics import infer_end_time
from app.analysis.events.models import EventExtraction, NormalizedEventExtraction
from app.analysis.events.prompt import PROMPT_VERSION, build_event_extraction_prompt

logger = logging.getLogger(__name__)


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


def _parse_hhmm(value: str | None) -> time | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Normalize common variants.
    # - If model emits HH:MM:SS, time.fromisoformat handles it.
    # - If model emits H:MM, time.fromisoformat handles it too.
    try:
        return time.fromisoformat(s)
    except ValueError:
        return None


def _extract_json_object(raw: str) -> dict:
    """Extract the first JSON object from a raw model response."""

    raw = (raw or "").strip()
    if not raw:
        raise ValueError("empty model response")

    # Fast path: direct JSON.
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Tolerant path: find {...} region.
    m = _JSON_OBJECT_RE.search(raw)
    if not m:
        raise ValueError("model response did not contain a JSON object")

    snippet = m.group(0)
    obj = json.loads(snippet)
    if not isinstance(obj, dict):
        raise ValueError("extracted JSON was not an object")
    return obj


_ALLOWED_EVENT_TYPES: dict[str, str] = {
    "theatre": "Theatre",
    "theater": "Theatre",
    "comedy": "Comedy",
    "opera": "Opera",
    "ballet": "Ballet",
    "cinema": "Cinema",
    "movie": "Cinema",
    "film": "Cinema",
    "social": "Social",
    "other": "Other",
}


def _normalize_event_type(value: str | None) -> str | None:
    """Normalize a model-provided event type to the canonical allowed set.

    Canonical values:
      Theatre, Comedy, Opera, Ballet, Cinema, Social, Other
    """

    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    # If it's already one of the canonical values, accept it.
    canonical = {"Theatre", "Comedy", "Opera", "Ballet", "Cinema", "Social", "Other"}
    if raw in canonical:
        return raw

    key = raw.casefold()

    # Map common synonyms.
    if key in _ALLOWED_EVENT_TYPES:
        return _ALLOWED_EVENT_TYPES[key]

    # Map older/previous prompt categories into the closest new bucket.
    legacy_to_new = {
        "concert": "Other",
        "gig": "Other",
        "music": "Other",
        "sports": "Other",
        "sport": "Other",
        "travel": "Other",
        "meeting": "Other",
        "dinner": "Social",
        "restaurant": "Social",
        "party": "Social",
        "appointment": "Other",
    }
    if key in legacy_to_new:
        return legacy_to_new[key]

    # If we can't confidently map it, return Other rather than leaking free-form strings.
    return "Other"


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


def extract_event_from_email(
    *,
    ollama_host: str,
    ollama_model: str,
    subject: str | None,
    from_domain: str | None,
    internal_date_iso: str | None,
    body: str,
) -> NormalizedEventExtraction:
    """Extract a single event from an email body.

    Returns a normalized object ready for DB persistence.

    Notes:
        We do not invent end times in the prompt. If end_time is missing but
        event_type/start_time/date are present, we infer a best-guess and mark it.
    """

    prompt = build_event_extraction_prompt(
        subject=subject,
        from_domain=from_domain,
        internal_date_iso=internal_date_iso,
        body=body,
    )

    raw = _call_ollama_generate(host=ollama_host, model=ollama_model, prompt=prompt)
    raw_obj = _extract_json_object(raw)

    parsed = EventExtraction.model_validate(raw_obj)

    normalized_event_type = _normalize_event_type(parsed.event_type)

    ev_date = _parse_iso_date(parsed.event_date)
    start_t = _parse_hhmm(parsed.start_time)
    end_t = _parse_hhmm(parsed.end_time)

    end_inferred = False
    if end_t is None and ev_date is not None and start_t is not None:
        inferred = infer_end_time(
            event_type=normalized_event_type,
            event_date=ev_date,
            start_time=start_t,
        )
        if inferred is not None:
            end_t = inferred
            end_inferred = True

    # Normalize blank strings to None.
    def _clean_str(v: str | None) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    return NormalizedEventExtraction(
        event_name=_clean_str(parsed.event_name),
        event_type=_clean_str(normalized_event_type),
        event_date=ev_date,
        start_time=start_t,
        end_time=end_t,
        timezone=_clean_str(parsed.timezone),
        end_time_inferred=end_inferred,
        confidence=parsed.confidence,
        raw_json=raw_obj,
        model=ollama_model,
        prompt_version=PROMPT_VERSION,
        notes=_clean_str(parsed.notes),
    )
