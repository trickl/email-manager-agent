"""Policy repository.

Stage 2 uses Postgres as the source of truth for policies.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from sqlalchemy import text

from app.policy.models import PolicyCadence, PolicyDefinitionV1, PolicyRecord, PolicyTriggerType


def _now() -> datetime:
    return datetime.now(timezone.utc)


def list_policies(engine) -> list[PolicyRecord]:
    q = text(
        """
        SELECT
            id::text,
            name,
            enabled,
            trigger_type,
            cadence,
            definition_json::text,
            created_at,
            updated_at
        FROM email_policy
        ORDER BY created_at ASC
        """
    )

    out: list[PolicyRecord] = []
    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()

    for r in rows:
        definition = PolicyDefinitionV1.model_validate_json(r[5])
        out.append(
            PolicyRecord(
                id=r[0],
                name=r[1],
                enabled=bool(r[2]),
                trigger_type=r[3],
                cadence=r[4],
                definition=definition,
                created_at=r[6].isoformat(),
                updated_at=r[7].isoformat(),
            )
        )

    return out


def get_policy(engine, policy_id: str) -> PolicyRecord | None:
    q = text(
        """
        SELECT
            id::text,
            name,
            enabled,
            trigger_type,
            cadence,
            definition_json::text,
            created_at,
            updated_at
        FROM email_policy
        WHERE id::text = :policy_id
        """
    )

    with engine.begin() as conn:
        row = conn.execute(q, {"policy_id": policy_id}).fetchone()

    if not row:
        return None

    definition = PolicyDefinitionV1.model_validate_json(row[5])
    return PolicyRecord(
        id=row[0],
        name=row[1],
        enabled=bool(row[2]),
        trigger_type=row[3],
        cadence=row[4],
        definition=definition,
        created_at=row[6].isoformat(),
        updated_at=row[7].isoformat(),
    )


def create_policy(
    engine,
    *,
    name: str,
    enabled: bool,
    trigger_type: PolicyTriggerType,
    cadence: PolicyCadence,
    definition: PolicyDefinitionV1,
) -> str:
    policy_id = str(uuid.uuid4())
    now = _now()

    q = text(
        """
        INSERT INTO email_policy (
            id,
            name,
            enabled,
            trigger_type,
            cadence,
            definition_json,
            created_at,
            updated_at
        )
        VALUES (
            :id::uuid,
            :name,
            :enabled,
            :trigger_type,
            :cadence,
            :definition_json::jsonb,
            :created_at,
            :updated_at
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "id": policy_id,
                "name": name,
                "enabled": enabled,
                "trigger_type": trigger_type,
                "cadence": cadence,
                "definition_json": definition.model_dump_json(),
                "created_at": now,
                "updated_at": now,
            },
        )

    return policy_id


def set_policy_enabled(engine, *, policy_id: str, enabled: bool) -> None:
    q = text(
        """
        UPDATE email_policy
        SET enabled = :enabled,
            updated_at = NOW()
        WHERE id::text = :policy_id
        """
    )

    with engine.begin() as conn:
        conn.execute(q, {"policy_id": policy_id, "enabled": enabled})


def ensure_default_policies(engine) -> None:
    """Seed a canonical Stage 2 policy if no policies exist.

    Canonical example:
      If category = Commercial & Marketing AND age > 180 days -> move to trash

    This is intentionally conservative and can be edited later.
    """

    check = text("SELECT COUNT(*)::int FROM email_policy")
    with engine.begin() as conn:
        n = conn.execute(check).scalar() or 0

    if n > 0:
        return

    definition = PolicyDefinitionV1(
        conditions=[
            {"type": "category_equals", "value": "Commercial & Marketing"},
            {"type": "age_days_gt", "days": 180},
        ],
        action={"type": "move_to_trash", "retention_days": 30},
    )

    _ = create_policy(
        engine,
        name="Trash old marketing (180d)",
        enabled=True,
        trigger_type="scheduled",
        cadence="weekly",
        definition=definition,
    )
