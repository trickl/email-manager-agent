"""Gmail label sync + retention APIs.

Responsibilities:
- Ensure taxonomy labels exist as Gmail labels (create/rename, store Gmail label ids).
- Push taxonomy label membership to Gmail messages (bulk and incremental via outbox).
- Run retention sweep that archives messages (non-destructive): remove INBOX and add an archive marker label.

This module intentionally avoids removed policy engine concepts.
"""

from __future__ import annotations

from datetime import datetime, timezone
import unicodedata

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.db.postgres import engine
from app.repository.taxonomy_admin_repository import (
    GMAIL_ARCHIVED_LABEL_NAME,
    TaxonomyAdminRepository,
    gmail_label_name,
)
from app.settings import Settings

router = APIRouter(prefix="/api/gmail-sync", tags=["gmail-sync"])


class SyncExistenceRequest(BaseModel):
    dry_run: bool = False


class SyncExistenceResponse(BaseModel):
    dry_run: bool
    created: int
    updated: int
    linked_existing: int
    errors: int
    generated_at: datetime


class PushBulkRequest(BaseModel):
    limit: int = Field(default=200, ge=1, le=5000)
    offset: int = Field(default=0, ge=0)


class PushResponse(BaseModel):
    attempted: int
    succeeded: int
    failed: int
    generated_at: datetime


class PushIncrementalRequest(BaseModel):
    limit: int = Field(default=200, ge=1, le=5000)


class RetentionPreviewRequest(BaseModel):
    limit: int = Field(default=25, ge=1, le=200)


class RetentionPreviewItem(BaseModel):
    message_id: int
    gmail_message_id: str
    subject: str | None = None
    from_domain: str
    internal_date: datetime


class RetentionPreviewResponse(BaseModel):
    eligible_count: int
    sample: list[RetentionPreviewItem]
    generated_at: datetime


class RetentionRunRequest(BaseModel):
    limit: int = Field(default=500, ge=1, le=5000)
    dry_run: bool = False


class RetentionPlanRequest(BaseModel):
    """Plan retention archive actions in the DB.

    This enqueues eligible messages into an outbox table. A separate long-running
    job later applies the Gmail marker label.
    """

    # Best-effort guardrail: allow callers to cap planning work if needed.
    # If None, we plan all currently-eligible messages.
    max_rows: int | None = Field(default=None, ge=1, le=5_000_000)


class RetentionPlanResponse(BaseModel):
    planned: int
    pending_outbox: int
    generated_at: datetime


class RetentionRunResponse(BaseModel):
    dry_run: bool
    attempted: int
    succeeded: int
    failed: int
    generated_at: datetime


class GmailAuthStatusResponse(BaseModel):
    token_scopes: list[str]
    has_modify: bool
    generated_at: datetime


def _gmail_service(*, modify: bool) -> object:
    from app.gmail.client import GMAIL_SCOPE_MODIFY, GMAIL_SCOPE_READONLY, get_gmail_service_from_files

    s = Settings()
    scopes = [GMAIL_SCOPE_MODIFY] if modify else [GMAIL_SCOPE_READONLY]
    return get_gmail_service_from_files(
        credentials_path=s.gmail_credentials_path,
        token_path=s.gmail_token_path,
        scopes=scopes,
        auth_mode=s.gmail_auth_mode,
        allow_interactive=s.gmail_allow_interactive,
    )


@router.get("/auth/status", response_model=GmailAuthStatusResponse)
def gmail_auth_status() -> GmailAuthStatusResponse:
    """Return the scopes currently recorded in token.json.

    This endpoint does not call Gmail; it only inspects the token file.
    """

    import json

    from app.gmail.client import GMAIL_SCOPE_MODIFY

    s = Settings()

    scopes: list[str] = []
    try:
        with open(s.gmail_token_path, "r", encoding="utf-8") as f:
            token_obj = json.load(f)
        raw = token_obj.get("scopes")
        if isinstance(raw, list):
            scopes = [str(x) for x in raw if x]
    except FileNotFoundError:
        scopes = []
    except Exception:
        # Best-effort: if parsing fails, surface empty rather than 500.
        scopes = []

    return GmailAuthStatusResponse(
        token_scopes=scopes,
        has_modify=GMAIL_SCOPE_MODIFY in set(scopes),
        generated_at=datetime.now(timezone.utc),
    )


@router.post("/labels/sync", response_model=SyncExistenceResponse)
def sync_label_existence(req: SyncExistenceRequest) -> SyncExistenceResponse:
    """Ensure all active taxonomy labels exist as Gmail labels and store gmail_label_id."""

    from app.gmail.client import create_label, label_name_to_id, list_label_names, update_label
    from googleapiclient.errors import HttpError

    s = Settings()
    service = _gmail_service(modify=True)

    repo = TaxonomyAdminRepository(engine)
    labels = [l for l in repo.list_labels(include_inactive=False) if l.is_active]
    by_id = {l.id: l for l in repo.list_labels(include_inactive=True)}

    name_to_id = label_name_to_id(service, user_id=s.gmail_user_id)
    id_to_name = list_label_names(service, user_id=s.gmail_user_id)

    def _norm_label_name(name: str) -> str:
        # Gmail treats some name variants as conflicting (e.g., case/whitespace differences).
        # We normalize to improve our ability to link to existing labels rather than repeatedly
        # attempting a create/rename that yields HttpError 409.
        return unicodedata.normalize("NFKC", str(name)).strip().casefold()

    name_to_id_norm: dict[str, str] = {_norm_label_name(k): str(v) for k, v in name_to_id.items()}

    def _lookup_existing_id(label_name: str) -> str | None:
        existing = name_to_id.get(label_name)
        if existing:
            return str(existing)
        return name_to_id_norm.get(_norm_label_name(label_name))

    def _http_status(err: Exception) -> int | None:
        try:
            resp = getattr(err, "resp", None)
            status = getattr(resp, "status", None)
            return int(status) if status is not None else None
        except Exception:
            return None

    def _refresh_label_maps() -> None:
        nonlocal name_to_id, id_to_name, name_to_id_norm
        name_to_id = label_name_to_id(service, user_id=s.gmail_user_id)
        id_to_name = list_label_names(service, user_id=s.gmail_user_id)
        name_to_id_norm = {_norm_label_name(k): str(v) for k, v in name_to_id.items()}

    created = 0
    updated = 0
    linked_existing = 0
    errors = 0

    for l in labels:
        parent = by_id.get(l.parent_id) if l.parent_id else None
        desired_name = gmail_label_name(label=l, parent=parent)

        try:
            if l.gmail_label_id:
                # If we know the Gmail id, rename if needed.
                current_name = id_to_name.get(l.gmail_label_id)

                # Stored mapping may be stale if labels were deleted in Gmail.
                # If the id is unknown, clear mapping and treat as missing.
                if current_name is None:
                    if not req.dry_run:
                        repo.set_gmail_sync_fields(
                            label_id=l.id,
                            gmail_label_id=None,
                            sync_status="stale",
                            sync_error="stored gmail_label_id not found in Gmail label list",
                        )
                    # Fall through to name-based link/create.
                else:
                    if current_name != desired_name:
                        if not req.dry_run:
                            try:
                                update_label(
                                    service,
                                    label_id=l.gmail_label_id,
                                    name=desired_name,
                                    user_id=s.gmail_user_id,
                                )
                                # Keep in-memory maps in sync for the remainder of this run.
                                id_to_name[str(l.gmail_label_id)] = str(desired_name)
                                name_to_id.pop(str(current_name), None)
                                name_to_id[str(desired_name)] = str(l.gmail_label_id)
                                name_to_id_norm[_norm_label_name(desired_name)] = str(l.gmail_label_id)
                            except HttpError as he:
                                # Common case when we've previously created the *new* label name
                                # (e.g. after removing a prefix) and we're now attempting to rename
                                # the old Gmail label to that already-existing name.
                                if _http_status(he) == 409:
                                    _refresh_label_maps()
                                    existing_id = _lookup_existing_id(desired_name)
                                    if existing_id:
                                        repo.set_gmail_sync_fields(
                                            label_id=l.id,
                                            gmail_label_id=str(existing_id),
                                            sync_status="ok",
                                        )
                                        linked_existing += 1
                                        continue
                                raise

                        repo.set_gmail_sync_fields(
                            label_id=l.id,
                            gmail_label_id=l.gmail_label_id,
                            sync_status="ok",
                        )
                        updated += 1
                    else:
                        repo.set_gmail_sync_fields(
                            label_id=l.id,
                            gmail_label_id=l.gmail_label_id,
                            sync_status="ok",
                        )
                    continue

            # No stored id: link by name or create.
            existing_id = _lookup_existing_id(desired_name)
            if existing_id:
                if not req.dry_run:
                    repo.set_gmail_sync_fields(
                        label_id=l.id,
                        gmail_label_id=str(existing_id),
                        sync_status="ok",
                    )
                linked_existing += 1
                continue

            if req.dry_run:
                created += 1
                continue

            try:
                created_label = create_label(service, name=desired_name, user_id=s.gmail_user_id)
            except HttpError as he:
                # Another process/run (or a previous partial run) may have created the label.
                # Treat 409 as a link-existing rather than a hard error.
                if _http_status(he) == 409:
                    _refresh_label_maps()
                    existing_id = _lookup_existing_id(desired_name)
                    if existing_id:
                        repo.set_gmail_sync_fields(
                            label_id=l.id,
                            gmail_label_id=str(existing_id),
                            sync_status="ok",
                        )
                        linked_existing += 1
                        continue
                raise

            gmail_id = str(created_label.get("id"))
            if gmail_id:
                repo.set_gmail_sync_fields(
                    label_id=l.id,
                    gmail_label_id=gmail_id,
                    sync_status="ok",
                )
                # Keep in-memory maps in sync for the remainder of this run.
                name_to_id[str(desired_name)] = str(gmail_id)
                name_to_id_norm[_norm_label_name(desired_name)] = str(gmail_id)
                id_to_name[str(gmail_id)] = str(desired_name)
            created += 1
        except Exception as e:
            errors += 1
            if not req.dry_run:
                repo.set_gmail_sync_fields(label_id=l.id, sync_status="error", sync_error=str(e))

    return SyncExistenceResponse(
        dry_run=req.dry_run,
        created=created,
        updated=updated,
        linked_existing=linked_existing,
        errors=errors,
        generated_at=datetime.now(timezone.utc),
    )


@router.post("/messages/push-bulk", response_model=PushResponse)
def push_labels_bulk(req: PushBulkRequest) -> PushResponse:
    """Bulk push taxonomy labels to Gmail messages based on DB assignments."""

    from sqlalchemy import text

    from app.gmail.client import modify_message_labels

    s = Settings()
    service = _gmail_service(modify=True)

    q = text(
        """
        SELECT
            em.id AS message_id,
            em.gmail_message_id AS gmail_message_id,
            ARRAY_AGG(mtl.taxonomy_label_id) AS taxonomy_label_ids
        FROM email_message em
        JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
        JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
        WHERE tl.is_active = TRUE
        GROUP BY em.id, em.gmail_message_id
        ORDER BY em.id ASC
        LIMIT :limit OFFSET :offset
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"limit": int(req.limit), "offset": int(req.offset)}).mappings().all()

    if not rows:
        return PushResponse(attempted=0, succeeded=0, failed=0, generated_at=datetime.now(timezone.utc))

    # Load mapping taxonomy_label_id -> gmail_label_id.
    label_ids: set[int] = set()
    for r in rows:
        for tid in (r["taxonomy_label_ids"] or []):
            label_ids.add(int(tid))

    with engine.begin() as conn:
        label_rows = conn.execute(
            text(
                """
                SELECT id, gmail_label_id
                FROM taxonomy_label
                WHERE id = ANY(:ids)
                """
            ),
            {"ids": list(label_ids)},
        ).fetchall()

    taxonomy_to_gmail: dict[int, str] = {
        int(r[0]): str(r[1]) for r in label_rows if r[1] is not None and str(r[1]).strip()
    }

    attempted = 0
    succeeded = 0
    failed = 0

    for r in rows:
        msg_id = int(r["message_id"])
        gmail_message_id = str(r["gmail_message_id"])
        tids = [int(x) for x in (r["taxonomy_label_ids"] or [])]
        add_ids = [taxonomy_to_gmail.get(tid) for tid in tids]
        add_ids = [x for x in add_ids if x]

        attempted += 1
        if not add_ids:
            # Missing mapping; treat as a failure (actionable).
            failed += 1
            continue

        try:
            modify_message_labels(
                service,
                message_id=gmail_message_id,
                add_label_ids=add_ids,
                remove_label_ids=None,
                user_id=s.gmail_user_id,
            )
            succeeded += 1
        except Exception:
            failed += 1

    return PushResponse(
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        generated_at=datetime.now(timezone.utc),
    )


@router.post("/messages/push-incremental", response_model=PushResponse)
def push_labels_incremental(req: PushIncrementalRequest) -> PushResponse:
    """Incremental push using outbox table."""

    from sqlalchemy import text

    from app.gmail.client import modify_message_labels

    s = Settings()
    service = _gmail_service(modify=True)

    with engine.begin() as conn:
        outbox = conn.execute(
            text(
                """
                SELECT o.id, o.message_id, em.gmail_message_id
                FROM label_push_outbox o
                JOIN email_message em ON em.id = o.message_id
                WHERE o.processed_at IS NULL
                ORDER BY o.created_at ASC
                LIMIT :limit
                """
            ),
            {"limit": int(req.limit)},
        ).mappings().all()

    if not outbox:
        return PushResponse(attempted=0, succeeded=0, failed=0, generated_at=datetime.now(timezone.utc))

    attempted = 0
    succeeded = 0
    failed = 0

    for o in outbox:
        outbox_id = int(o["id"])
        message_id = int(o["message_id"])
        gmail_message_id = str(o["gmail_message_id"])

        attempted += 1
        try:
            # Compute current active taxonomy labels for the message.
            with engine.begin() as conn:
                tids = conn.execute(
                    text(
                        """
                        SELECT mtl.taxonomy_label_id
                        FROM message_taxonomy_label mtl
                        JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
                        WHERE mtl.message_id = :mid AND tl.is_active = TRUE
                        """
                    ),
                    {"mid": message_id},
                ).fetchall()

                tset = [int(r[0]) for r in tids]
                label_rows = conn.execute(
                    text(
                        """
                        SELECT id, gmail_label_id
                        FROM taxonomy_label
                        WHERE id = ANY(:ids)
                        """
                    ),
                    {"ids": list(tset)},
                ).fetchall()

                taxonomy_to_gmail = {
                    int(r[0]): str(r[1]) for r in label_rows if r[1] is not None and str(r[1]).strip()
                }

            add_ids = [taxonomy_to_gmail.get(tid) for tid in tset]
            add_ids = [x for x in add_ids if x]

            if not add_ids:
                raise RuntimeError("missing gmail label mapping for message")

            modify_message_labels(
                service,
                message_id=gmail_message_id,
                add_label_ids=add_ids,
                remove_label_ids=None,
                user_id=s.gmail_user_id,
            )

            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE label_push_outbox
                        SET processed_at = NOW(), error = NULL
                        WHERE id = :id
                        """
                    ),
                    {"id": outbox_id},
                )

            succeeded += 1
        except Exception as e:
            failed += 1
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE label_push_outbox
                        SET processed_at = NOW(), error = :error
                        WHERE id = :id
                        """
                    ),
                    {"id": outbox_id, "error": str(e)[:5000]},
                )

    return PushResponse(
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        generated_at=datetime.now(timezone.utc),
    )


@router.post("/retention/preview", response_model=RetentionPreviewResponse)
def retention_preview(req: RetentionPreviewRequest) -> RetentionPreviewResponse:
    """Preview messages eligible for retention archive sweep (Option A).

    Retention is computed relative to the email's received time (email_message.internal_date),
    not when the taxonomy label was assigned.
    """

    from sqlalchemy import text

    from app.repository.pipeline_kv_repository import get_retention_default_days

    default_days = int(get_retention_default_days(engine))

    q_count = text(
        """
        SELECT COUNT(DISTINCT em.id)
        FROM email_message em
        JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
        JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
                LEFT JOIN taxonomy_label p ON p.id = tl.parent_id
        WHERE em.archived_at IS NULL
                    AND em.internal_date <= (
                        NOW() - (COALESCE(tl.retention_days, p.retention_days, :default_days)::text || ' days')::interval
                    )
        """
    )

    q_sample = text(
        """
        SELECT DISTINCT
            em.id,
            em.gmail_message_id,
            em.subject,
            em.from_domain,
            em.internal_date
        FROM email_message em
        JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
        JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
                LEFT JOIN taxonomy_label p ON p.id = tl.parent_id
        WHERE em.archived_at IS NULL
                    AND em.internal_date <= (
                        NOW() - (COALESCE(tl.retention_days, p.retention_days, :default_days)::text || ' days')::interval
                    )
        ORDER BY em.internal_date DESC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        eligible = int(conn.execute(q_count, {"default_days": int(default_days)}).scalar() or 0)
        rows = conn.execute(
            q_sample,
            {"limit": int(req.limit), "default_days": int(default_days)},
        ).fetchall()

    sample = [
        RetentionPreviewItem(
            message_id=int(r[0]),
            gmail_message_id=str(r[1]),
            subject=r[2],
            from_domain=str(r[3]),
            internal_date=r[4],
        )
        for r in rows
    ]

    return RetentionPreviewResponse(
        eligible_count=eligible,
        sample=sample,
        generated_at=datetime.now(timezone.utc),
    )


@router.post("/retention/run", response_model=RetentionRunResponse)
def retention_run(req: RetentionRunRequest) -> RetentionRunResponse:
    """Run retention archive sweep (Option A).

    Retention is computed relative to the email's received time (email_message.internal_date),
    not when the taxonomy label was assigned.
    """

    from sqlalchemy import text

    from app.gmail.client import create_label, label_name_to_id, modify_message_labels
    from app.repository.pipeline_kv_repository import get_retention_default_days

    s = Settings()
    service = _gmail_service(modify=True)

    default_days = int(get_retention_default_days(engine))

    # Ensure archive label exists.
    name_to_id = label_name_to_id(service, user_id=s.gmail_user_id)
    archived_label_id = name_to_id.get(GMAIL_ARCHIVED_LABEL_NAME)
    if not archived_label_id and not req.dry_run:
        try:
            created = create_label(service, name=GMAIL_ARCHIVED_LABEL_NAME, user_id=s.gmail_user_id)
            archived_label_id = str(created.get("id"))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"failed to ensure archive label: {e}") from e

    q = text(
        """
        SELECT DISTINCT em.id, em.gmail_message_id
        FROM email_message em
        JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
        JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
                LEFT JOIN taxonomy_label p ON p.id = tl.parent_id
        WHERE em.archived_at IS NULL
                    AND em.internal_date <= (
                        NOW() - (COALESCE(tl.retention_days, p.retention_days, :default_days)::text || ' days')::interval
                    )
        ORDER BY em.id ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(
            q,
            {"limit": int(req.limit), "default_days": int(default_days)},
        ).fetchall()

    attempted = 0
    succeeded = 0
    failed = 0

    for msg_id, gmail_message_id in rows:
        attempted += 1
        try:
            if not req.dry_run:
                modify_message_labels(
                    service,
                    message_id=str(gmail_message_id),
                    add_label_ids=[archived_label_id] if archived_label_id else None,
                    remove_label_ids=["INBOX"],
                    user_id=s.gmail_user_id,
                )
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE email_message SET archived_at = NOW() WHERE id = :id"),
                        {"id": int(msg_id)},
                    )
            succeeded += 1
        except Exception:
            failed += 1

    return RetentionRunResponse(
        dry_run=req.dry_run,
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        generated_at=datetime.now(timezone.utc),
    )


@router.post("/retention/plan", response_model=RetentionPlanResponse)
def retention_plan(req: RetentionPlanRequest) -> RetentionPlanResponse:
    """Plan retention archive actions in Postgres (DB-only).

    This computes eligibility using the email received timestamp (email_message.internal_date)
    and effective retention (Tier-2 -> Tier-1 -> Tier-0 default), and enqueues messages into
    archive_push_outbox.

    This endpoint does NOT call Gmail.
    """

    from sqlalchemy import text

    from app.repository.pipeline_kv_repository import get_retention_default_days
    from app.repository.retention_archive_repository import count_pending_outbox, plan_archive_outbox

    default_days = int(get_retention_default_days(engine))

    # Optional safety cap.
    if req.max_rows is not None:
        # We implement the cap by selecting a bounded set of eligible message ids.
        # This is still idempotent due to UNIQUE(message_id) in the outbox.
        q = text(
            """
            WITH eligible AS (
                SELECT DISTINCT em.id AS message_id
                FROM email_message em
                JOIN message_taxonomy_label mtl ON mtl.message_id = em.id
                JOIN taxonomy_label tl ON tl.id = mtl.taxonomy_label_id
                LEFT JOIN taxonomy_label p ON p.id = tl.parent_id
                WHERE em.archived_at IS NULL
                  AND em.gmail_message_id IS NOT NULL
                                    AND em.internal_date <= (
                    NOW() - (COALESCE(tl.retention_days, p.retention_days, :default_days)::text || ' days')::interval
                  )
                ORDER BY em.id ASC
                LIMIT :limit
            )
            INSERT INTO archive_push_outbox (message_id, reason)
            SELECT e.message_id, 'retention_eligible'
            FROM eligible e
            ON CONFLICT (message_id)
            DO UPDATE SET
                created_at = NOW(),
                processed_at = NULL,
                error = NULL
            RETURNING message_id
            """
        )

        with engine.begin() as conn:
            planned_rows = conn.execute(
                q,
                {"default_days": int(default_days), "limit": int(req.max_rows)},
            ).fetchall()
        planned = int(len(planned_rows))
    else:
        planned = int(plan_archive_outbox(engine=engine, default_days=default_days))

    pending = int(count_pending_outbox(engine=engine))
    return RetentionPlanResponse(
        planned=planned,
        pending_outbox=pending,
        generated_at=datetime.now(timezone.utc),
    )
