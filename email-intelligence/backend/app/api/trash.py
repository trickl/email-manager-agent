"""Trash (Stage 1 lifecycle) API.

Provides a read/write view of Postgres-backed lifecycle state.

Important:
- This does not modify Gmail/provider state.
- Untrash is reversible and logged to `email_action_log`.
"""

from __future__ import annotations

from fastapi import APIRouter

from app.api.models import BulkUntrashRequest, BulkUntrashResponse, TrashListResponse, TrashedMessageSummary
from app.db.postgres import engine
from app.repository import trash_repository

router = APIRouter(prefix="/api/trash", tags=["trash"])


@router.get("", response_model=TrashListResponse)
def list_trash(limit: int = 200, offset: int = 0) -> TrashListResponse:
    limit = max(1, min(int(limit), 1000))
    offset = max(0, int(offset))

    total = trash_repository.count_trashed(engine)
    rows = trash_repository.list_trashed_messages(engine, limit=limit, offset=offset)

    messages = [TrashedMessageSummary(**r) for r in rows]
    return TrashListResponse(total=total, limit=limit, offset=offset, messages=messages)


@router.post("/untrash", response_model=BulkUntrashResponse)
def bulk_untrash(body: BulkUntrashRequest) -> BulkUntrashResponse:
    ids = [i for i in body.gmail_message_ids if i]
    updated = trash_repository.mark_messages_untrashed(engine, gmail_message_ids=ids)

    # Best-effort audit logging.
    for mid in ids:
        trash_repository.log_action(
            engine,
            action_type="untrash",
            gmail_message_id=mid,
            policy_id=None,
            from_state="TRASHED",
            to_state="ACTIVE",
        )

    return BulkUntrashResponse(untrashed_count=updated)
