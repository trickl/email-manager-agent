"""Cluster labeling implementations.

We require a local LLM (Ollama) for labeling.

The Tier-1 taxonomy is enforced: exactly one of the known categories must be chosen.
"""

from __future__ import annotations

import json
import logging
import re
import urllib.request
from dataclasses import dataclass

from app.labeling.prompt import build_label_prompt
from app.labeling.tier2 import TIER2_SEED
from app.labeling.tier1 import TIER1_CATEGORIES, validate_tier1_category

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LabelResult:
    category: str
    subcategory: str | None


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
        tier2_options: dict[str, list[str]] | None = None,
    ) -> LabelResult:
        raise NotImplementedError


_PREFIX_RE = re.compile(r"^\s*(category|tier\s*1|tier\s*2|subcategory)\s*:\s*", re.I)
_BULLET_RE = re.compile(r"^\s*(?:[-*•]+|\d+\.|\d+\))\s*")


def _normalize_response_line(line: str) -> str:
    s = _PREFIX_RE.sub("", line).strip()
    s = _BULLET_RE.sub("", s).strip()
    return s


def _parse_multiline_label_response(
    raw: str,
    *,
    tier2_options: dict[str, list[str]] | None = None,
) -> tuple[str, str | None]:
    """Parse the LLM response.

    Expected format (strict contract):
        <Tier-1 category>\n
        <Tier-2 subcategory or None>

    Tolerant extensions:
      - allow "Category: ..." / "Subcategory: ..." prefixes
      - allow extra leading/trailing lines; we pick the first Tier-1 match we can find
    """

    lines = [_normalize_response_line(l) for l in raw.splitlines()]
    lines = [l for l in lines if l]
    if not lines:
        raise ValueError("Ollama returned empty response")

    # Precompute a fallback map from the static seed, so we can still recover even if
    # tier2_options is missing/incomplete for any reason.
    _seed_tier2_to_tier1: dict[str, str] = {}
    for cat, subs in TIER2_SEED.items():
        for sub_name, _desc in subs:
            _seed_tier2_to_tier1[sub_name.casefold()] = cat

    def _tier2_match_for_category(category_name: str, candidate: str) -> str | None:
        cand = candidate.strip()
        if not cand:
            return None
        # Prefer canonical spelling from DB-provided taxonomy.
        if tier2_options:
            for s in tier2_options.get(category_name, []):
                if cand.casefold() == s.casefold():
                    return s
        # Fall back to seed spelling.
        for seed_name, seed_cat in _seed_tier2_to_tier1.items():
            if seed_cat == category_name and cand.casefold() == seed_name:
                # Return the original candidate to preserve casing if we don't have a canonical.
                return cand
        return None

    def _tier2_to_tier1(candidate: str) -> tuple[str, str] | None:
        """If candidate looks like a Tier-2 label, infer its parent Tier-1 category."""

        cand = candidate.strip()
        if not cand:
            return None

        # First: try DB-provided taxonomy.
        if tier2_options:
            for cat, subs in tier2_options.items():
                for s in subs:
                    if cand.casefold() == s.casefold():
                        return cat, s

        # Fallback: static seed taxonomy.
        seed_cat = _seed_tier2_to_tier1.get(cand.casefold())
        if seed_cat:
            return seed_cat, cand

        return None

    category: str | None = None
    category_idx: int | None = None

    # 1) Look for a Tier-1 category anywhere (exact match).
    for i, line in enumerate(lines):
        for c in TIER1_CATEGORIES:
            if line.casefold() == c.casefold():
                category = c
                category_idx = i
                break
        if category is not None:
            break

    # 2) Look for a Tier-1 category as a substring (tolerant to extra words).
    if category is None:
        for i, line in enumerate(lines):
            folded = line.casefold()
            for c in TIER1_CATEGORIES:
                if c.casefold() in folded:
                    category = c
                    category_idx = i
                    break
            if category is not None:
                break

    if category is None:
        # Common failure mode: the model returns a Tier-2 label in line 1.
        # Another: it returns a Tier-2 label on the first non-empty line.
        for i, line in enumerate(lines):
            inferred = _tier2_to_tier1(line)
            if inferred is not None:
                inferred_cat, inferred_sub = inferred
                return inferred_cat, inferred_sub

        # Fall back to strict Tier-1 validation.
        category = validate_tier1_category(lines[0])
        category_idx = 0

    subcategory: str | None = None
    if category_idx is not None:
        for j in range(category_idx + 1, len(lines)):
            cand = lines[j].strip()
            if not cand:
                continue
            if cand.casefold() in {"none", "null", "(none)"}:
                subcategory = None
            else:
                subcategory = cand
            break

        # Another common failure mode: the model outputs Tier-2 then Tier-1.
        # Example:
        #   Tickets & Bookings
        #   Financial
        # In that case, we find Tier-1 at index 1 but miss the Tier-2 (it is above).
        if subcategory is None and category_idx > 0:
            maybe_prev = lines[category_idx - 1].strip()
            matched = _tier2_match_for_category(category, maybe_prev)
            if matched is not None:
                subcategory = matched

        # If the chosen subcategory is actually a known Tier-2 option under this category,
        # prefer the canonical spelling from the taxonomy.
        if subcategory is not None:
            matched = _tier2_match_for_category(category, subcategory)
            if matched is not None:
                subcategory = matched

    return category, subcategory


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
        tier2_options: dict[str, list[str]] | None = None,
    ) -> LabelResult:
        prompt = build_label_prompt(
            sender_domain=sender_domain,
            subject_examples=subject_examples,
            cluster_size=cluster_size,
            frequency_label=frequency_label,
            unread_label=unread_label,
            bodies=bodies,
            tier2_options=tier2_options,
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

        category, subcategory = _parse_multiline_label_response(raw, tier2_options=tier2_options)
        category = validate_tier1_category(category)
        if subcategory is not None:
            subcategory = str(subcategory).strip() or None

        return LabelResult(category=category, subcategory=subcategory)


def build_labeler(*, ollama_host: str | None, ollama_model: str) -> ClusterLabeler:
    if ollama_host:
        logger.info(
            "labeler_using_ollama",
            extra={"host": ollama_host, "model": ollama_model},
        )
        return OllamaLabeler(host=ollama_host, model=ollama_model)

    raise RuntimeError(
        "Ollama is not configured. Set EMAIL_INTEL_OLLAMA_HOST (e.g. http://localhost:11434)."
    )
