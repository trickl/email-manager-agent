"""Policies (Stage 2 deterministic engine) API."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db.postgres import engine
from app.policy.models import PolicyCadence, PolicyDefinitionV1, PolicyRecord, PolicyTriggerType
from app.repository.policy_repository import create_policy, get_policy, list_policies, set_policy_enabled

router = APIRouter(prefix="/api/policies", tags=["policies"])


class CreatePolicyRequest(BaseModel):
    name: str
    enabled: bool = True
    trigger_type: PolicyTriggerType = "scheduled"
    cadence: PolicyCadence = "weekly"
    definition: PolicyDefinitionV1


class SetEnabledRequest(BaseModel):
    enabled: bool


@router.get("", response_model=list[PolicyRecord])
def api_list_policies() -> list[PolicyRecord]:
    return list_policies(engine)


@router.get("/{policy_id}", response_model=PolicyRecord)
def api_get_policy(policy_id: str) -> PolicyRecord:
    rec = get_policy(engine, policy_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"Unknown policy_id: {policy_id}")
    return rec


@router.post("", response_model=PolicyRecord)
def api_create_policy(body: CreatePolicyRequest) -> PolicyRecord:
    policy_id = create_policy(
        engine,
        name=body.name,
        enabled=body.enabled,
        trigger_type=body.trigger_type,
        cadence=body.cadence,
        definition=body.definition,
    )
    rec = get_policy(engine, policy_id)
    if rec is None:
        raise HTTPException(status_code=500, detail="Failed to fetch created policy")
    return rec


@router.post("/{policy_id}/enabled", response_model=PolicyRecord)
def api_set_policy_enabled(policy_id: str, body: SetEnabledRequest) -> PolicyRecord:
    rec = get_policy(engine, policy_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"Unknown policy_id: {policy_id}")

    set_policy_enabled(engine, policy_id=policy_id, enabled=bool(body.enabled))

    updated = get_policy(engine, policy_id)
    if updated is None:
        raise HTTPException(status_code=500, detail="Failed to fetch updated policy")
    return updated
