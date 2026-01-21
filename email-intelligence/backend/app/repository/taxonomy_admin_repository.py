"""Taxonomy CRUD + Gmail mapping helpers.

This repository supports the taxonomy-driven labeling + Gmail sync approach:
- Taxonomy labels are managed in Postgres.
- Gmail labels are derived deterministically from taxonomy name hierarchy.
- Gmail label IDs are stored for efficient message label application.

This module intentionally avoids the removed policy engine concepts.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TaxonomyLabelRow:
    id: int
    level: int
    slug: str
    name: str
    description: str
    parent_id: int | None
    retention_days: int | None
    is_active: bool
    managed_by_system: bool
    gmail_label_id: str | None
    last_sync_at: str | None
    sync_status: str | None
    sync_error: str | None


# Gmail label naming policy
#
# We intentionally do NOT prefix taxonomy labels (e.g. no "Email Intelligence/") so that
# the Gmail label tree mirrors the taxonomy hierarchy directly:
#   Tier-1: <Tier1 Name>
#   Tier-2: <Tier1 Name>/<Tier2 Name>
#
# Retention marker label (used only by retention sweep / archive-push outbox).
# NOTE: Gmail rejects the bare names "Archive" and "Archived" with:
#   HttpError 400: "Invalid label name"
# so we use a slightly more specific marker label name.
GMAIL_ARCHIVED_LABEL_NAME = "Email Archive"


def gmail_label_name(*, label: TaxonomyLabelRow, parent: TaxonomyLabelRow | None) -> str:
    """Derive a deterministic Gmail label name for a taxonomy label."""

    if label.level == 1 or not label.parent_id:
        # Keep Tier-1 names shallow.
        return f"{label.name}"

    parent_name = parent.name if parent else "(Unknown)"
    return f"{parent_name}/{label.name}"


class TaxonomyAdminRepository:
    def __init__(self, engine):
        self._engine = engine

    _UNSET: object = object()

    def list_labels(self, *, include_inactive: bool = True) -> list[TaxonomyLabelRow]:
        from sqlalchemy import text

        where = "" if include_inactive else "WHERE tl.is_active = TRUE"

        q = text(
            f"""
            SELECT
                tl.id,
                tl.level,
                tl.slug,
                tl.name,
                tl.description,
                tl.parent_id,
                tl.retention_days,
                tl.is_active,
                tl.managed_by_system,
                tl.gmail_label_id,
                tl.last_sync_at,
                tl.sync_status,
                tl.sync_error
            FROM taxonomy_label tl
            {where}
            ORDER BY tl.level ASC, tl.parent_id NULLS FIRST, tl.name ASC
            """
        )

        with self._engine.begin() as conn:
            rows = conn.execute(q).mappings().all()

        return [
            TaxonomyLabelRow(
                id=int(r["id"]),
                level=int(r["level"]),
                slug=str(r["slug"]),
                name=str(r["name"]),
                description=str(r["description"] or ""),
                parent_id=int(r["parent_id"]) if r["parent_id"] is not None else None,
                retention_days=int(r["retention_days"]) if r["retention_days"] is not None else None,
                is_active=bool(r["is_active"]),
                managed_by_system=bool(r["managed_by_system"]),
                gmail_label_id=str(r["gmail_label_id"]) if r["gmail_label_id"] else None,
                last_sync_at=str(r["last_sync_at"]) if r["last_sync_at"] else None,
                sync_status=str(r["sync_status"]) if r["sync_status"] else None,
                sync_error=str(r["sync_error"]) if r["sync_error"] else None,
            )
            for r in rows
        ]

    def get_label(self, label_id: int) -> TaxonomyLabelRow | None:
        from sqlalchemy import text

        q = text(
            """
            SELECT
                tl.id,
                tl.level,
                tl.slug,
                tl.name,
                tl.description,
                tl.parent_id,
                tl.retention_days,
                tl.is_active,
                tl.managed_by_system,
                tl.gmail_label_id,
                tl.last_sync_at,
                tl.sync_status,
                tl.sync_error
            FROM taxonomy_label tl
            WHERE tl.id = :id
            """
        )

        with self._engine.begin() as conn:
            r = conn.execute(q, {"id": int(label_id)}).mappings().first()

        if not r:
            return None

        return TaxonomyLabelRow(
            id=int(r["id"]),
            level=int(r["level"]),
            slug=str(r["slug"]),
            name=str(r["name"]),
            description=str(r["description"] or ""),
            parent_id=int(r["parent_id"]) if r["parent_id"] is not None else None,
            retention_days=int(r["retention_days"]) if r["retention_days"] is not None else None,
            is_active=bool(r["is_active"]),
            managed_by_system=bool(r["managed_by_system"]),
            gmail_label_id=str(r["gmail_label_id"]) if r["gmail_label_id"] else None,
            last_sync_at=str(r["last_sync_at"]) if r["last_sync_at"] else None,
            sync_status=str(r["sync_status"]) if r["sync_status"] else None,
            sync_error=str(r["sync_error"]) if r["sync_error"] else None,
        )

    def create_label(
        self,
        *,
        name: str,
        description: str = "",
        parent_id: int | None = None,
        retention_days: int | None = None,
        is_active: bool = True,
    ) -> TaxonomyLabelRow:
        """Create a taxonomy label.

        Slug is derived from the name and namespaced under parent when present.
        """

        from sqlalchemy import text

        from app.labeling.tier2 import slugify

        with self._engine.begin() as conn:
            parent: dict | None = None
            if parent_id is not None:
                parent = conn.execute(
                    text("SELECT id, slug, name FROM taxonomy_label WHERE id = :id"),
                    {"id": int(parent_id)},
                ).mappings().first()
                if not parent:
                    raise ValueError(f"parent taxonomy_label id={parent_id} not found")

            level = 1 if parent_id is None else 2
            base_slug = slugify(name)
            slug = base_slug if parent_id is None else f"{parent['slug']}--{base_slug}"

            r = conn.execute(
                text(
                    """
                    INSERT INTO taxonomy_label (
                        level,
                        slug,
                        name,
                        description,
                        parent_id,
                        retention_days,
                        is_active,
                        managed_by_system
                    )
                    VALUES (
                        :level,
                        :slug,
                        :name,
                        :description,
                        :parent_id,
                        :retention_days,
                        :is_active,
                        TRUE
                    )
                    RETURNING
                        id,
                        level,
                        slug,
                        name,
                        description,
                        parent_id,
                        retention_days,
                        is_active,
                        managed_by_system,
                        gmail_label_id,
                        last_sync_at,
                        sync_status,
                        sync_error
                    """
                ),
                {
                    "level": int(level),
                    "slug": str(slug),
                    "name": str(name),
                    "description": str(description or ""),
                    "parent_id": int(parent_id) if parent_id is not None else None,
                    "retention_days": int(retention_days) if retention_days is not None else None,
                    "is_active": bool(is_active),
                },
            ).mappings().first()

        assert r is not None
        return TaxonomyLabelRow(
            id=int(r["id"]),
            level=int(r["level"]),
            slug=str(r["slug"]),
            name=str(r["name"]),
            description=str(r["description"] or ""),
            parent_id=int(r["parent_id"]) if r["parent_id"] is not None else None,
            retention_days=int(r["retention_days"]) if r["retention_days"] is not None else None,
            is_active=bool(r["is_active"]),
            managed_by_system=bool(r["managed_by_system"]),
            gmail_label_id=str(r["gmail_label_id"]) if r["gmail_label_id"] else None,
            last_sync_at=str(r["last_sync_at"]) if r["last_sync_at"] else None,
            sync_status=str(r["sync_status"]) if r["sync_status"] else None,
            sync_error=str(r["sync_error"]) if r["sync_error"] else None,
        )

    def update_label(
        self,
        *,
        label_id: int,
        name: str | None = None,
        description: str | None = None,
        retention_days: int | None | object = _UNSET,
        is_active: bool | None = None,
    ) -> TaxonomyLabelRow | None:
        from sqlalchemy import text

        # Build partial update without overwriting unspecified fields.
        sets: list[str] = []
        params: dict[str, object] = {"id": int(label_id)}

        if name is not None:
            sets.append("name = :name")
            params["name"] = str(name)
        if description is not None:
            sets.append("description = :description")
            params["description"] = str(description)
        if retention_days is not self._UNSET:
            sets.append("retention_days = :retention_days")
            params["retention_days"] = (
                int(retention_days) if retention_days is not None else None
            )
        if is_active is not None:
            sets.append("is_active = :is_active")
            params["is_active"] = bool(is_active)

        if not sets:
            return self.get_label(label_id)

        q = text(
            f"""
            UPDATE taxonomy_label
            SET {', '.join(sets)}
            WHERE id = :id
            RETURNING
                id,
                level,
                slug,
                name,
                description,
                parent_id,
                retention_days,
                is_active,
                managed_by_system,
                gmail_label_id,
                last_sync_at,
                sync_status,
                sync_error
            """
        )

        with self._engine.begin() as conn:
            r = conn.execute(q, params).mappings().first()

        if not r:
            return None

        return TaxonomyLabelRow(
            id=int(r["id"]),
            level=int(r["level"]),
            slug=str(r["slug"]),
            name=str(r["name"]),
            description=str(r["description"] or ""),
            parent_id=int(r["parent_id"]) if r["parent_id"] is not None else None,
            retention_days=int(r["retention_days"]) if r["retention_days"] is not None else None,
            is_active=bool(r["is_active"]),
            managed_by_system=bool(r["managed_by_system"]),
            gmail_label_id=str(r["gmail_label_id"]) if r["gmail_label_id"] else None,
            last_sync_at=str(r["last_sync_at"]) if r["last_sync_at"] else None,
            sync_status=str(r["sync_status"]) if r["sync_status"] else None,
            sync_error=str(r["sync_error"]) if r["sync_error"] else None,
        )

    def delete_label(self, *, label_id: int) -> bool:
        from sqlalchemy import text

        with self._engine.begin() as conn:
            res = conn.execute(text("DELETE FROM taxonomy_label WHERE id = :id"), {"id": int(label_id)})
        return bool(res.rowcount and res.rowcount > 0)

    def bulk_set_retention_days(self, *, items: list[tuple[int, int | None]]) -> int:
        """Bulk update retention_days for multiple taxonomy labels.

        Args:
            items: List of (taxonomy_label_id, retention_days). retention_days may be None to clear.

        Returns:
            Number of labels updated.
        """

        if not items:
            return 0

        from sqlalchemy import text

        ids = [int(i[0]) for i in items]
        days = [int(i[1]) if i[1] is not None else None for i in items]

        q = text(
            """
            WITH data AS (
                SELECT *
                FROM UNNEST(CAST(:ids AS int[]), CAST(:days AS int[])) AS t(id, retention_days)
            )
            UPDATE taxonomy_label tl
            SET retention_days = data.retention_days
            FROM data
            WHERE tl.id = data.id
            RETURNING tl.id
            """
        )

        with self._engine.begin() as conn:
            rows = conn.execute(q, {"ids": ids, "days": days}).fetchall()

        return int(len(rows))

    def set_gmail_sync_fields(
        self,
        *,
        label_id: int,
        gmail_label_id: str | None | object = _UNSET,
        sync_status: str | None = None,
        sync_error: str | None = None,
    ) -> None:
        """Update Gmail mapping and last sync metadata."""

        from sqlalchemy import text

        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE taxonomy_label
                    SET
                        gmail_label_id = CASE
                            WHEN :gmail_label_id_is_set THEN :gmail_label_id
                            ELSE gmail_label_id
                        END,
                        last_sync_at = NOW(),
                        sync_status = :sync_status,
                        sync_error = :sync_error
                    WHERE id = :id
                    """
                ),
                {
                    "id": int(label_id),
                    "gmail_label_id_is_set": gmail_label_id is not self._UNSET,
                    "gmail_label_id": None
                    if gmail_label_id is self._UNSET
                    else (str(gmail_label_id) if gmail_label_id is not None else None),
                    "sync_status": str(sync_status) if sync_status is not None else None,
                    "sync_error": str(sync_error) if sync_error is not None else None,
                },
            )
