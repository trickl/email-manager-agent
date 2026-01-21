"""Taxonomy management API.

This API replaces the removed policy engine concepts.
It provides CRUD operations over taxonomy labels, which are later synced to Gmail labels.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.db.postgres import engine
from app.repository.pipeline_kv_repository import get_retention_default_days, set_retention_default_days
from app.repository.taxonomy_admin_repository import (
    TaxonomyAdminRepository,
    TaxonomyLabelRow,
    gmail_label_name,
)
from app.repository.taxonomy_cleanup_repository import (
    TaxonomyCleanupSuggestion,
    list_cleanup_suggestions,
    merge_taxonomy_labels,
)

router = APIRouter(prefix="/api/taxonomy", tags=["taxonomy"])


class TaxonomyLabelResponse(BaseModel):
    id: int
    level: int
    slug: str
    name: str
    description: str
    parent_id: int | None = None
    retention_days: int | None = None
    is_active: bool
    managed_by_system: bool

    gmail_label_id: str | None = None
    gmail_label_name: str

    last_sync_at: str | None = None
    sync_status: str | None = None
    sync_error: str | None = None

    assigned_message_count: int = 0


class CreateTaxonomyLabelRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)
    parent_id: int | None = None
    retention_days: int | None = Field(default=None, ge=1, le=3650)
    is_active: bool = True


class UpdateTaxonomyLabelRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    retention_days: int | None = Field(default=None, ge=1, le=3650)
    is_active: bool | None = None


class BulkRetentionUpdateItem(BaseModel):
    id: int = Field(..., ge=1)
    retention_days: int | None = Field(default=None, ge=1, le=3650)


class BulkRetentionUpdateRequest(BaseModel):
    items: list[BulkRetentionUpdateItem] = Field(default_factory=list, max_length=2000)


class BulkRetentionUpdateResponse(BaseModel):
    updated: int


class RetentionDefaultResponse(BaseModel):
    retention_default_days: int = Field(..., ge=1, le=3650)


class TaxonomyCleanupSuggestionResponse(BaseModel):
    kind: str
    reason: str
    source_id: int
    source_name: str
    source_parent_id: int | None = None
    source_assigned_count: int
    target_id: int | None = None
    target_name: str | None = None


class MergeTaxonomyLabelsRequest(BaseModel):
    source_id: int = Field(..., ge=1)
    target_id: int = Field(..., ge=1)
    enqueue_outbox: bool = True


class MergeTaxonomyLabelsResponse(BaseModel):
    moved_assignments: int
    deleted_source: int


def _to_response(
    label: TaxonomyLabelRow,
    *,
    parent_by_id: dict[int, TaxonomyLabelRow],
    assigned_count_by_id: dict[int, int],
) -> TaxonomyLabelResponse:
    parent = parent_by_id.get(label.parent_id) if label.parent_id else None
    return TaxonomyLabelResponse(
        id=label.id,
        level=label.level,
        slug=label.slug,
        name=label.name,
        description=label.description,
        parent_id=label.parent_id,
        retention_days=label.retention_days,
        is_active=label.is_active,
        managed_by_system=label.managed_by_system,
        gmail_label_id=label.gmail_label_id,
        gmail_label_name=gmail_label_name(label=label, parent=parent),
        last_sync_at=label.last_sync_at,
        sync_status=label.sync_status,
        sync_error=label.sync_error,
        assigned_message_count=int(assigned_count_by_id.get(label.id, 0)),
    )


def _assigned_counts() -> dict[int, int]:
    from sqlalchemy import text

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


@router.get("", response_model=list[TaxonomyLabelResponse])
def list_taxonomy_labels(
    include_inactive: bool = Query(True, description="Include inactive labels"),
) -> list[TaxonomyLabelResponse]:
    repo = TaxonomyAdminRepository(engine)
    labels = repo.list_labels(include_inactive=include_inactive)
    parent_by_id = {l.id: l for l in labels}
    assigned_count_by_id = _assigned_counts()

    return [
        _to_response(l, parent_by_id=parent_by_id, assigned_count_by_id=assigned_count_by_id)
        for l in labels
    ]


@router.post("", response_model=TaxonomyLabelResponse)
def create_taxonomy_label(req: CreateTaxonomyLabelRequest) -> TaxonomyLabelResponse:
    repo = TaxonomyAdminRepository(engine)
    try:
        label = repo.create_label(
            name=req.name,
            description=req.description,
            parent_id=req.parent_id,
            retention_days=req.retention_days,
            is_active=req.is_active,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    labels = repo.list_labels(include_inactive=True)
    parent_by_id = {l.id: l for l in labels}

    return _to_response(
        label,
        parent_by_id=parent_by_id,
        assigned_count_by_id=_assigned_counts(),
    )


@router.put("/{label_id}", response_model=TaxonomyLabelResponse)
def update_taxonomy_label(label_id: int, req: UpdateTaxonomyLabelRequest) -> TaxonomyLabelResponse:
    repo = TaxonomyAdminRepository(engine)

    # Pydantic doesn't distinguish between "unset" and explicit null by value alone.
    # Use fields-set tracking so callers can clear retention_days by sending
    # {"retention_days": null}.
    fields_set = getattr(req, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(req, "__fields_set__", set())
    retention_is_set = "retention_days" in (fields_set or set())

    label = repo.update_label(
        label_id=label_id,
        name=req.name,
        description=req.description,
        retention_days=req.retention_days if retention_is_set else repo._UNSET,
        is_active=req.is_active,
    )
    if not label:
        raise HTTPException(status_code=404, detail="taxonomy label not found")

    labels = repo.list_labels(include_inactive=True)
    parent_by_id = {l.id: l for l in labels}

    return _to_response(
        label,
        parent_by_id=parent_by_id,
        assigned_count_by_id=_assigned_counts(),
    )


@router.delete("/{label_id}")
def delete_taxonomy_label(label_id: int) -> dict:
    repo = TaxonomyAdminRepository(engine)
    ok = repo.delete_label(label_id=label_id)
    if not ok:
        raise HTTPException(status_code=404, detail="taxonomy label not found")
    return {"deleted": True, "id": int(label_id)}


@router.post("/retention/bulk", response_model=BulkRetentionUpdateResponse)
def bulk_update_retention(req: BulkRetentionUpdateRequest) -> BulkRetentionUpdateResponse:
    """Bulk update retention_days for many taxonomy labels.

    This exists to support the Categories UI where operators set retention as value+unit (e.g. 2 months).
    """

    repo = TaxonomyAdminRepository(engine)
    items = [(int(i.id), (int(i.retention_days) if i.retention_days is not None else None)) for i in req.items]
    updated = repo.bulk_set_retention_days(items=items)
    return BulkRetentionUpdateResponse(updated=int(updated))


@router.get("/retention/default", response_model=RetentionDefaultResponse)
def get_retention_default() -> RetentionDefaultResponse:
    days = int(get_retention_default_days(engine))
    # Keep within API bounds.
    days = max(1, min(3650, days))
    return RetentionDefaultResponse(retention_default_days=days)


@router.put("/retention/default", response_model=RetentionDefaultResponse)
def set_retention_default(req: RetentionDefaultResponse) -> RetentionDefaultResponse:
    set_retention_default_days(engine, int(req.retention_default_days))
    return RetentionDefaultResponse(retention_default_days=int(req.retention_default_days))


@router.get("/cleanup/suggestions", response_model=list[TaxonomyCleanupSuggestionResponse])
def get_cleanup_suggestions() -> list[TaxonomyCleanupSuggestionResponse]:
    """List suspicious taxonomy labels and suggested merges/deletes.

    This is intended to help operators clean up labels that were accidentally created
    from malformed LLM output.
    """

    items = list_cleanup_suggestions(engine)
    return [
        TaxonomyCleanupSuggestionResponse(
            kind=i.kind,
            reason=i.reason,
            source_id=i.source_id,
            source_name=i.source_name,
            source_parent_id=i.source_parent_id,
            source_assigned_count=i.source_assigned_count,
            target_id=i.target_id,
            target_name=i.target_name,
        )
        for i in items
    ]


@router.post("/merge", response_model=MergeTaxonomyLabelsResponse)
def merge_taxonomy_labels_endpoint(req: MergeTaxonomyLabelsRequest) -> MergeTaxonomyLabelsResponse:
    """Merge one Tier-2 taxonomy label into another.

    This moves assignments (message -> taxonomy_label) and deletes the source label.
    """

    try:
        res = merge_taxonomy_labels(
            engine,
            source_id=req.source_id,
            target_id=req.target_id,
            enqueue_outbox=req.enqueue_outbox,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return MergeTaxonomyLabelsResponse(
        moved_assignments=int(res.get("moved_assignments", 0)),
        deleted_source=int(res.get("deleted_source", 0)),
    )
