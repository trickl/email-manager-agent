"""Tier-1 taxonomy (enforced).

There is intentionally no "Unknown" category.
"""

from __future__ import annotations

TIER1_CATEGORIES: tuple[str, ...] = (
    "Financial",
    "Commercial & Marketing",
    "Work & Professional",
    "Personal & Social",
    "Account & Identity",
    "System & Automated",
)


def validate_tier1_category(category: str) -> str:
    if category not in TIER1_CATEGORIES:
        raise ValueError(
            f"Invalid category '{category}'. Must be one of: {', '.join(TIER1_CATEGORIES)}"
        )
    return category
