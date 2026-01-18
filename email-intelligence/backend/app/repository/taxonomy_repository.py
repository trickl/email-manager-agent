"""Taxonomy persistence helpers.

This module enforces the existence of a Tier-1 taxonomy seed in Postgres.
It is intentionally idempotent to support repeatable startup and incremental development.

There is intentionally no "unknown" label. If a cluster doesn't fit, create a new
label rather than defaulting to an "Unknown" bucket.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TaxonomyLabelSeed:
    """Seed definition for a taxonomy label."""

    level: int
    slug: str
    name: str
    description: str


TIER1_SEED: tuple[TaxonomyLabelSeed, ...] = (
    TaxonomyLabelSeed(
        level=1,
        slug="financial",
        name="Financial",
        description=(
            "Records, requests, or confirmations of financial transactions or obligations."
        ),
    ),
    TaxonomyLabelSeed(
        level=1,
        slug="commercial-marketing",
        name="Commercial & Marketing",
        description=(
            "Influences purchasing or engagement decisions (includes legitimate newsletters and promotions)."
        ),
    ),
    TaxonomyLabelSeed(
        level=1,
        slug="work-professional",
        name="Work & Professional",
        description="Related to employment, collaboration, or professional identity.",
    ),
    TaxonomyLabelSeed(
        level=1,
        slug="personal-social",
        name="Personal & Social",
        description=(
            "Personal relationships, community, education, and non-billing healthcare communications."
        ),
    ),
    TaxonomyLabelSeed(
        level=1,
        slug="account-identity",
        name="Account & Identity",
        description=(
            "Manages access, identity, or account state (login alerts, password resets, confirmations)."
        ),
    ),
    TaxonomyLabelSeed(
        level=1,
        slug="system-automated",
        name="System & Automated",
        description=(
            "Machine-generated state/event/failure notifications (GitHub, monitoring, CI/CD, SaaS)."
        ),
    ),
)


def ensure_taxonomy_schema(engine) -> None:
    """Ensure the taxonomy table exists.

    Args:
        engine: SQLAlchemy engine bound to the Postgres database.
    """

    from sqlalchemy import text

    ddl = text(
        """
        CREATE TABLE IF NOT EXISTS taxonomy_label (
            id SERIAL PRIMARY KEY,

            level SMALLINT NOT NULL CHECK (level >= 1),
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',

            parent_id INTEGER REFERENCES taxonomy_label(id) ON DELETE RESTRICT,

            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_taxonomy_label_level
            ON taxonomy_label(level);

        CREATE INDEX IF NOT EXISTS idx_taxonomy_label_parent
            ON taxonomy_label(parent_id);
        """
    )

    with engine.begin() as conn:
        conn.execute(ddl)


def seed_tier1_taxonomy(engine) -> None:
    """Insert the Tier-1 taxonomy seed (idempotent).

    Args:
        engine: SQLAlchemy engine bound to the Postgres database.
    """

    from sqlalchemy import text

    insert = text(
        """
        INSERT INTO taxonomy_label (level, slug, name, description, parent_id)
        VALUES (:level, :slug, :name, :description, NULL)
        ON CONFLICT (slug) DO NOTHING
        """
    )

    with engine.begin() as conn:
        for label in TIER1_SEED:
            conn.execute(
                insert,
                {
                    "level": label.level,
                    "slug": label.slug,
                    "name": label.name,
                    "description": label.description,
                },
            )


def ensure_taxonomy_seeded(engine) -> None:
    """Ensure schema exists and the Tier-1 taxonomy is present."""

    ensure_taxonomy_schema(engine)
    seed_tier1_taxonomy(engine)
