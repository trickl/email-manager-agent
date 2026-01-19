"""Policy evaluation engine (Stage 2: deterministic rules v1).

This module translates PolicyDefinitionV1 into a SQL predicate and executes bulk
operations against Postgres.

Important: Stage 2 uses AND-only semantics for conditions.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from app.policy.models import PolicyCondition, PolicyDefinitionV1


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _build_where(conditions: list[PolicyCondition]) -> tuple[str, dict[str, object]]:
    clauses: list[str] = ["lifecycle_state = 'ACTIVE'"]
    params: dict[str, object] = {}

    idx = 0
    for c in conditions:
        idx += 1
        if c.type == "category_equals":
            key = f"cat_{idx}"
            clauses.append("category = :" + key)
            params[key] = c.value or ""
        elif c.type == "subcategory_equals":
            key = f"sub_{idx}"
            clauses.append("subcategory = :" + key)
            params[key] = c.value or ""
        elif c.type == "from_domain_equals":
            key = f"dom_{idx}"
            clauses.append("from_domain = :" + key)
            params[key] = c.value or ""
        elif c.type == "subject_contains":
            key = f"subj_{idx}"
            clauses.append("COALESCE(subject, '') ILIKE :" + key)
            params[key] = f"%{c.value or ''}%"
        elif c.type == "age_days_gt":
            days = int(c.days or 0)
            cutoff = _now() - timedelta(days=days)
            key = f"cutoff_{idx}"
            clauses.append("internal_date < :" + key)
            params[key] = cutoff
        elif c.type == "is_unread_equals":
            key = f"unread_{idx}"
            clauses.append("is_unread = :" + key)
            params[key] = bool(c.flag)
        else:
            raise ValueError(f"Unsupported condition type: {c.type}")

    return " AND ".join(clauses), params


def select_matching_message_ids(
    engine,
    *,
    definition: PolicyDefinitionV1,
    limit: int | None = None,
) -> list[str]:
    where, params = _build_where(definition.conditions)

    lim = ""
    if limit is not None:
        lim = "\nLIMIT :limit"
        params["limit"] = int(limit)

    q = text(
        f"""
        SELECT gmail_message_id
        FROM email_message
        WHERE {where}
        ORDER BY internal_date ASC
        {lim}
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, params).fetchall()

    return [r[0] for r in rows if r and r[0]]
