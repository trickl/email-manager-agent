"""Policy models (Stage 2: deterministic rules v1).

We store policy definitions as JSON in Postgres (email_policy.definition_json) so the schema can
iterate without frequent migrations.

Stage 2 scope:
- deterministic AND-only conditions
- scheduled/batch evaluation (weekly)
- action: move to trash (soft delete) with retention

Stage 3+ extends with OR/UNLESS and on-ingest evaluation.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


PolicyTriggerType = Literal["scheduled", "on_ingest"]
PolicyCadence = Literal["daily", "weekly", "monthly"]


class PolicyCondition(BaseModel):
    """A single deterministic predicate.

    This intentionally stays small in Stage 2.
    """

    type: Literal[
        "category_equals",
        "subcategory_equals",
        "from_domain_equals",
        "subject_contains",
        "age_days_gt",
        "is_unread_equals",
    ]

    # Only one of these will be used depending on condition type.
    value: str | None = None
    days: int | None = None
    flag: bool | None = None


class TrashAction(BaseModel):
    """Soft-delete action."""

    type: Literal["move_to_trash"] = "move_to_trash"
    retention_days: int = Field(default=30, ge=1, le=3650)


class PolicyDefinitionV1(BaseModel):
    """Policy definition payload stored in Postgres."""

    version: Literal["v1"] = "v1"
    conditions: list[PolicyCondition] = Field(default_factory=list)
    action: TrashAction


class PolicyRecord(BaseModel):
    """Convenience model representing a row from email_policy."""

    id: str
    name: str
    enabled: bool
    trigger_type: PolicyTriggerType
    cadence: PolicyCadence
    definition: PolicyDefinitionV1
    created_at: str
    updated_at: str
