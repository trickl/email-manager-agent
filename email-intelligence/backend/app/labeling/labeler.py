"""Cluster labeling implementations.

We support an optional local LLM (Ollama) and a deterministic fallback.

The Tier-1 taxonomy is enforced: exactly one of the known categories must be chosen.
"""

from __future__ import annotations

import json
import logging
import re
import urllib.request
from dataclasses import dataclass

from app.labeling.prompt import build_label_prompt
from app.labeling.tier1 import TIER1_CATEGORIES, validate_tier1_category

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LabelResult:
    category: str
    subcategory: str | None
    confidence: float


class ClusterLabeler:
    """Interface for cluster labeling."""

    def label(
        self,
        *,
        sender_domain: str,
        subject_examples: list[str],
        cluster_size: int,
        frequency_label: str,
        unread_label: str,
        bodies: list[str],
    ) -> LabelResult:
        raise NotImplementedError


class DeterministicFallbackLabeler(ClusterLabeler):
    """Deterministic fallback when no LLM is configured.

    This is intentionally conservative and explicit: it chooses the "least wrong" category
    using stable keyword cues. Confidence is kept low.
    """

    _FINANCIAL = re.compile(r"\b(invoice|receipt|statement|payment|tax|subscription charge)\b", re.I)
    _ACCOUNT = re.compile(r"\b(password|login|security|verify|verification|otp|2fa|account)\b", re.I)
    _SYSTEM = re.compile(r"\b(ci|build|failed|alert|notification|monitoring)\b", re.I)
    _MARKETING = re.compile(r"\b(newsletter|promo|promotion|discount|offer|sale|event)\b", re.I)
    _WORK = re.compile(r"\b(meeting|project|client|recruiter|job|interview)\b", re.I)

    def label(
        self,
        *,
        sender_domain: str,
        subject_examples: list[str],
        cluster_size: int,
        frequency_label: str,
        unread_label: str,
        bodies: list[str],
    ) -> LabelResult:
        text = "\n".join([sender_domain, *subject_examples, *bodies])

        category = "Commercial & Marketing"  # default "least wrong" for most noisy email
        if self._FINANCIAL.search(text):
            category = "Financial"
        elif self._ACCOUNT.search(text):
            category = "Account & Identity"
        elif self._SYSTEM.search(text) or sender_domain.endswith("github.com"):
            category = "System & Automated"
        elif self._WORK.search(text):
            category = "Work & Professional"
        elif self._MARKETING.search(text):
            category = "Commercial & Marketing"

        validate_tier1_category(category)
        return LabelResult(category=category, subcategory=None, confidence=0.51)


class OllamaLabeler(ClusterLabeler):
    """Label clusters via a local Ollama instance (optional)."""

    def __init__(self, *, host: str, model: str) -> None:
        self._host = host.rstrip("/")
        self._model = model

    def label(
        self,
        *,
        sender_domain: str,
        subject_examples: list[str],
        cluster_size: int,
        frequency_label: str,
        unread_label: str,
        bodies: list[str],
    ) -> LabelResult:
        prompt = build_label_prompt(
            sender_domain=sender_domain,
            subject_examples=subject_examples,
            cluster_size=cluster_size,
            frequency_label=frequency_label,
            unread_label=unread_label,
            bodies=bodies,
        )

        payload = json.dumps(
            {
                "model": self._model,
                "prompt": prompt,
                "stream": False,
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            url=f"{self._host}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8"))

        raw = (data.get("response") or "").strip()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Ollama returned non-JSON content: {raw[:200]}") from exc

        category = validate_tier1_category(str(parsed.get("category")))
        subcategory = parsed.get("subcategory")
        if subcategory is not None:
            subcategory = str(subcategory).strip() or None

        confidence = float(parsed.get("confidence") or 0.0)
        if confidence < 0.0 or confidence > 1.0:
            confidence = 0.0

        return LabelResult(category=category, subcategory=subcategory, confidence=confidence)


def build_labeler(*, ollama_host: str | None, ollama_model: str) -> ClusterLabeler:
    if ollama_host:
        logger.info(
            "labeler_using_ollama",
            extra={"host": ollama_host, "model": ollama_model},
        )
        return OllamaLabeler(host=ollama_host, model=ollama_model)

    logger.warning(
        "labeler_using_fallback",
        extra={"taxonomy": list(TIER1_CATEGORIES)},
    )
    return DeterministicFallbackLabeler()
