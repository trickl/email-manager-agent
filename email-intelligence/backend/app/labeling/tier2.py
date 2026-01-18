"""Tier-2 taxonomy (preferred, evolvable).

Tier-1 categories are enforced (see :mod:`app.labeling.tier1`). Tier-2 is a *preferred* set of
subcategories under each Tier-1 category.

Design goals:
- Provide the model with a strong initial set of subcategories (avoid "pot luck").
- Allow the model to extend the Tier-2 taxonomy when none are a good fit.
- Persist extensions so future prompts always include the latest taxonomy.

This module defines the initial Tier-2 seed used to populate Postgres.
"""

from __future__ import annotations

import re

from app.labeling.tier1 import TIER1_CATEGORIES


# Tier-2 seed: category -> ordered list of (subcategory, description)
TIER2_SEED: dict[str, tuple[tuple[str, str], ...]] = {
    "Financial": (
        ("Receipts", "One-off purchase confirmations"),
        ("Orders & Purchases", "Order confirmations, purchase details (non-recurring)"),
        ("Payments & Reminders", "Payment due notices, payment reminders, outstanding balance"),
        ("Tickets & Bookings", "Ticketing, bookings, reservations with a financial component"),
        ("Invoices & Bills", "Requests for payment (utilities, services)"),
        ("Statements", "Periodic summaries (bank, credit card, investment)"),
        ("Subscriptions", "Recurring charges (software, media, memberships)"),
        ("Taxes & Legal", "Tax documents, filings, official notices"),
        ("Refunds & Adjustments", "Chargebacks, refunds, corrections"),
    ),
    "Commercial & Marketing": (
        ("Newsletters", "Regular informational/promotional mailings"),
        ("Promotions & Offers", "Discounts, sales, limited offers"),
        ("Product Updates", "New features, launches, announcements"),
        ("Events & Webinars", "Invitations, registrations, reminders"),
        ("Surveys & Feedback", "Requests for reviews, ratings, opinions"),
    ),
    "Work & Professional": (
        ("Internal Communication", "Colleagues, team updates, internal notices"),
        ("Project & Client Updates", "Deliverables, status reports, coordination"),
        ("Recruitment", "Job applications, recruiters, interviews"),
        ("Professional Networks", "LinkedIn, industry groups, associations"),
        ("Training & Education", "Courses, certifications, learning platforms"),
    ),
    "Personal & Social": (
        ("Friends & Family", "Direct personal correspondence"),
        ("Health & Care", "Appointments, results, providers (non-billing)"),
        ("Education", "Schools, universities, learning (non-work)"),
        ("Clubs & Communities", "Hobbies, societies, local groups"),
        (
            "Travel & Leisure",
            "Bookings, itineraries, leisure activities (non-financial content)",
        ),
    ),
    "Account & Identity": (
        ("Security Alerts", "Login warnings, suspicious activity"),
        ("Authentication", "Password resets, 2FA codes"),
        ("Account Changes", "Email changes, profile updates"),
        ("Policy & Terms", "Terms of service, privacy updates"),
        ("Account Notifications", "General account status messages"),
    ),
    "System & Automated": (
        ("Code & DevOps", "GitHub, CI/CD, build systems"),
        ("Monitoring & Alerts", "System health, uptime, errors"),
        ("Forum & Platform Notifications", "Replies, mentions, moderation"),
        ("Scheduled Reports", "Automated digests, summaries"),
        ("Integration Events", "Webhooks, API-driven notifications"),
    ),
}


def slugify(value: str) -> str:
    """Convert a label name into a stable slug.

    The slug is used as a unique key in Postgres. We keep it predictable and ASCII-ish.

    Args:
        value: Label name.

    Returns:
        A normalized slug (lowercase, hyphen-separated).
    """

    v = value.strip().lower()
    v = v.replace("&", "and")
    v = re.sub(r"[^a-z0-9]+", "-", v)
    v = re.sub(r"-+", "-", v)
    return v.strip("-")


def validate_tier2_seed() -> None:
    """Validate that Tier-2 seed aligns with Tier-1 categories."""

    missing = [c for c in TIER1_CATEGORIES if c not in TIER2_SEED]
    extra = [c for c in TIER2_SEED if c not in set(TIER1_CATEGORIES)]
    if missing or extra:
        raise ValueError(f"Tier-2 seed mismatch missing={missing} extra={extra}")
