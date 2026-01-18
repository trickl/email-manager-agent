"""Taxonomy persistence helpers.

This module enforces the existence of a Tier-1 taxonomy seed in Postgres.
It is intentionally idempotent to support repeatable startup and incremental development.

There is intentionally no "unknown" label. If a cluster doesn't fit, create a new
label rather than defaulting to an "Unknown" bucket.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.labeling.tier2 import TIER2_SEED, slugify, validate_tier2_seed


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


_TIER1_SLUG_BY_NAME: dict[str, str] = {l.name: l.slug for l in TIER1_SEED}


def _tier2_slug(*, parent_slug: str, subcategory_name: str) -> str:
    # Ensure uniqueness across tiers by namespacing Tier-2 under Tier-1.
    # Example: financial--invoices-and-bills
    return f"{parent_slug}--{slugify(subcategory_name)}"


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


def seed_tier2_taxonomy(engine) -> None:
    """Insert the Tier-2 taxonomy seed (idempotent).

    Tier-2 rows reference Tier-1 rows via parent_id.

    Args:
        engine: SQLAlchemy engine bound to the Postgres database.
    """

    validate_tier2_seed()

    from sqlalchemy import text

    parent_q = text(
        """
        SELECT id
        FROM taxonomy_label
        WHERE level = 1 AND slug = :slug
        """
    )

    insert = text(
        """
        INSERT INTO taxonomy_label (level, slug, name, description, parent_id)
        VALUES (2, :slug, :name, :description, :parent_id)
        ON CONFLICT (slug) DO NOTHING
        """
    )

    with engine.begin() as conn:
        for category_name, subs in TIER2_SEED.items():
            parent_slug = _TIER1_SLUG_BY_NAME.get(category_name)
            if not parent_slug:
                # Should not happen due to validate_tier2_seed, but keep robust.
                continue

            parent_id = conn.execute(parent_q, {"slug": parent_slug}).scalar()
            if parent_id is None:
                continue

            for sub_name, sub_desc in subs:
                conn.execute(
                    insert,
                    {
                        "slug": _tier2_slug(parent_slug=parent_slug, subcategory_name=sub_name),
                        "name": sub_name,
                        "description": sub_desc,
                        "parent_id": int(parent_id),
                    },
                )


def list_tier2_options(engine) -> dict[str, list[str]]:
    """Return the current Tier-2 taxonomy (from Postgres).

    Returns:
        Mapping of Tier-1 category name -> ordered list of Tier-2 subcategory names.
    """

    from sqlalchemy import text

    q = text(
        """
        SELECT
            p.name AS category,
            c.name AS subcategory
        FROM taxonomy_label c
        JOIN taxonomy_label p ON p.id = c.parent_id
        WHERE c.level = 2
        ORDER BY p.name ASC, c.name ASC
        """
    )

    out: dict[str, list[str]] = {}
    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()

    for r in rows:
        cat = str(r[0])
        sub = str(r[1])
        out.setdefault(cat, []).append(sub)

    # Ensure all Tier-1 categories have a key (even if empty).
    for cat in _TIER1_SLUG_BY_NAME:
        out.setdefault(cat, [])

    return out


def ensure_tier2_label(
    engine,
    *,
    category_name: str,
    subcategory_name: str,
    description: str = "",
) -> None:
    """Ensure a Tier-2 subcategory exists (create if missing).

    This supports taxonomy extension: if the model returns a subcategory that isn't in the
    current Tier-2 list, we insert it under the chosen Tier-1 category.

    Args:
        engine: SQLAlchemy engine bound to Postgres.
        category_name: Tier-1 category name.
        subcategory_name: Tier-2 subcategory name.
        description: Optional description.
    """

    parent_slug = _TIER1_SLUG_BY_NAME.get(category_name)
    if not parent_slug:
        return

    sub = subcategory_name.strip()
    if not sub:
        return

    # Keep names reasonably bounded for UI + storage. (DB column is TEXT, but UX matters.)
    if len(sub) > 80:
        sub = sub[:80].rstrip()

    from sqlalchemy import text

    parent_q = text(
        """
        SELECT id
        FROM taxonomy_label
        WHERE level = 1 AND slug = :slug
        """
    )
    insert = text(
        """
        INSERT INTO taxonomy_label (level, slug, name, description, parent_id)
        VALUES (2, :slug, :name, :description, :parent_id)
        ON CONFLICT (slug) DO NOTHING
        """
    )

    with engine.begin() as conn:
        parent_id = conn.execute(parent_q, {"slug": parent_slug}).scalar()
        if parent_id is None:
            return

        conn.execute(
            insert,
            {
                "slug": _tier2_slug(parent_slug=parent_slug, subcategory_name=sub),
                "name": sub,
                "description": description,
                "parent_id": int(parent_id),
            },
        )


def ensure_taxonomy_seeded(engine) -> None:
    """Ensure schema exists and the Tier-1 taxonomy is present."""

    ensure_taxonomy_schema(engine)
    seed_tier1_taxonomy(engine)
    seed_tier2_taxonomy(engine)
