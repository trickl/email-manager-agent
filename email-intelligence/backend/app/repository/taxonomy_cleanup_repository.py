"""Taxonomy cleanup helpers.

Used to consolidate malformed Tier-2 taxonomy labels that were accidentally created from
LLM output (e.g. "Tier-2 Subcategory: X" or "Note: ...").

These operations are intentionally explicit and transactional.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import text


@dataclass(frozen=True)
class TaxonomyCleanupSuggestion:
    kind: str  # "merge" | "delete"
    reason: str
    source_id: int
    source_name: str
    source_parent_id: int | None
    source_assigned_count: int
    target_id: int | None = None
    target_name: str | None = None


_TIER2_PREFIX_RE = re.compile(r"^\s*tier\s*[-\s]*2\s*subcategory\s*:\s*(.+)$", re.I)
_SUBCATEGORY_PREFIX_RE = re.compile(r"^\s*subcategory\s*:\s*(.+)$", re.I)


def _assigned_counts(engine) -> dict[int, int]:
    q = text(
        """
        SELECT taxonomy_label_id, COUNT(*) AS cnt
        FROM message_taxonomy_label
        GROUP BY taxonomy_label_id
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()
    return {int(r[0]): int(r[1]) for r in rows}


def _find_label_by_name(engine, *, parent_id: int | None, name: str) -> tuple[int, str] | None:
    q = text(
        """
        SELECT id, name
        FROM taxonomy_label
        WHERE parent_id IS NOT DISTINCT FROM :parent_id
          AND lower(name) = lower(:name)
        LIMIT 1
        """
    )
    with engine.begin() as conn:
        r = conn.execute(q, {"parent_id": parent_id, "name": name}).first()
    if not r:
        return None
    return int(r[0]), str(r[1])


def list_cleanup_suggestions(engine) -> list[TaxonomyCleanupSuggestion]:
    """Suggest merges/deletes for suspicious taxonomy labels."""

    # Restrict to Tier-2: Tier-1 is enforced and seeded.
    q = text(
        """
        SELECT id, name, parent_id
        FROM taxonomy_label
        WHERE level = 2
        ORDER BY name ASC
        """
    )

    counts = _assigned_counts(engine)
    out: list[TaxonomyCleanupSuggestion] = []

    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()

    for r in rows:
        lid = int(r[0])
        name = str(r[1])
        parent_id = int(r[2]) if r[2] is not None else None
        assigned = int(counts.get(lid, 0))

        folded = name.casefold().strip()

        # "Note: ..." is always garbage in this system.
        if folded.startswith("note:") or folded.startswith("notes:") or (
            "chosen categories" in folded and "match" in folded
        ):
            out.append(
                TaxonomyCleanupSuggestion(
                    kind="delete",
                    reason="meta_note",
                    source_id=lid,
                    source_name=name,
                    source_parent_id=parent_id,
                    source_assigned_count=assigned,
                )
            )
            continue

        # Tier-2 prefix leaks: prefer the shorter name.
        m = _TIER2_PREFIX_RE.match(name)
        if not m:
            m = _SUBCATEGORY_PREFIX_RE.match(name)
        if m:
            stripped = m.group(1).strip()
            target = _find_label_by_name(engine, parent_id=parent_id, name=stripped)
            if target and target[0] != lid:
                out.append(
                    TaxonomyCleanupSuggestion(
                        kind="merge",
                        reason="prefix_leak",
                        source_id=lid,
                        source_name=name,
                        source_parent_id=parent_id,
                        source_assigned_count=assigned,
                        target_id=int(target[0]),
                        target_name=str(target[1]),
                    )
                )
                continue

            # If no target exists, still suggest delete (or manual rename/merge).
            out.append(
                TaxonomyCleanupSuggestion(
                    kind="delete",
                    reason="prefix_leak_no_target",
                    source_id=lid,
                    source_name=name,
                    source_parent_id=parent_id,
                    source_assigned_count=assigned,
                )
            )
            continue

    return out


def merge_taxonomy_labels(
    engine,
    *,
    source_id: int,
    target_id: int,
    enqueue_outbox: bool = True,
) -> dict[str, int]:
    """Merge a source taxonomy label into a target label.

    Moves message assignments from source -> target, then deletes the source label.

    Notes:
      - Gmail removal of old labels is not performed (current sync is additive). If you
        want Gmail cleanup, delete the old Gmail label after merge and re-sync.
    """

    if int(source_id) == int(target_id):
        raise ValueError("source_id and target_id must be different")

    # Enforce that both labels exist and are Tier-2 under the same parent.
    q = text(
        """
        SELECT id, level, parent_id, name
        FROM taxonomy_label
        WHERE id IN (:source_id, :target_id)
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"source_id": int(source_id), "target_id": int(target_id)}).fetchall()

    by_id = {int(r[0]): {"level": int(r[1]), "parent_id": r[2], "name": str(r[3])} for r in rows}
    if int(source_id) not in by_id:
        raise ValueError("source taxonomy label not found")
    if int(target_id) not in by_id:
        raise ValueError("target taxonomy label not found")

    s = by_id[int(source_id)]
    t = by_id[int(target_id)]
    if s["level"] != 2 or t["level"] != 2:
        raise ValueError("only Tier-2 labels can be merged")
    if s["parent_id"] != t["parent_id"]:
        raise ValueError("can only merge Tier-2 labels under the same Tier-1 parent")

    # Transactionally move assignments; keep existing target assignments.
    with engine.begin() as conn:
        moved_count = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM message_taxonomy_label
                    WHERE taxonomy_label_id = :source_id
                    """
                ),
                {"source_id": int(source_id)},
            ).scalar()
            or 0
        )

        conn.execute(
            text(
                """
                INSERT INTO message_taxonomy_label (message_id, taxonomy_label_id, assigned_at, confidence)
                SELECT message_id, :target_id, assigned_at, confidence
                FROM message_taxonomy_label
                WHERE taxonomy_label_id = :source_id
                ON CONFLICT (message_id, taxonomy_label_id)
                DO UPDATE SET
                    confidence = COALESCE(EXCLUDED.confidence, message_taxonomy_label.confidence)
                """
            ),
            {"source_id": int(source_id), "target_id": int(target_id)},
        )

        if enqueue_outbox and moved_count:
            # Enqueue incremental Gmail push for affected messages (adds the *target* label).
            # This does not remove the old Gmail label (sync is additive).
            conn.execute(
                text(
                    """
                    INSERT INTO label_push_outbox (message_id, reason)
                    SELECT message_id, 'taxonomy_merge'
                    FROM message_taxonomy_label
                    WHERE taxonomy_label_id = :source_id
                    """
                ),
                {"source_id": int(source_id)},
            )

        conn.execute(
            text("DELETE FROM message_taxonomy_label WHERE taxonomy_label_id = :source_id"),
            {"source_id": int(source_id)},
        )

        deleted = conn.execute(
            text("DELETE FROM taxonomy_label WHERE id = :source_id"),
            {"source_id": int(source_id)},
        ).rowcount

    return {
        "moved_assignments": moved_count,
        "deleted_source": int(deleted or 0),
    }
