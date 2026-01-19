"""Trash (soft-delete) repository + audit logging.

Stage 1 goal:
- represent deletion as TRASHED state in Postgres
- provide list + bulk undelete
- keep an audit log of actions

Provider operations (Gmail trash/untrash/delete) are performed by higher-level services.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text


def _now() -> datetime:
    return datetime.now(timezone.utc)


def list_trashed_messages(
    engine,
    *,
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    q = text(
        """
        SELECT
            gmail_message_id,
            subject,
            from_domain,
            internal_date,
            is_unread,
            category,
            subcategory,
            trashed_at,
            expiry_at,
            trashed_by_policy_id::text
        FROM email_message
        WHERE lifecycle_state = 'TRASHED'
        ORDER BY trashed_at DESC NULLS LAST, internal_date DESC
        LIMIT :limit
        OFFSET :offset
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"limit": limit, "offset": offset}).fetchall()

    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "gmail_message_id": r[0],
                "subject": r[1],
                "from_domain": r[2],
                "internal_date": r[3],
                "is_unread": bool(r[4]),
                "category": r[5],
                "subcategory": r[6],
                "trashed_at": r[7],
                "expiry_at": r[8],
                "trashed_by_policy_id": r[9],
            }
        )
    return out


def count_trashed(engine) -> int:
    q = text("SELECT COUNT(*)::int FROM email_message WHERE lifecycle_state = 'TRASHED'")
    with engine.begin() as conn:
        return int(conn.execute(q).scalar() or 0)


def mark_messages_trashed(
    engine,
    *,
    gmail_message_ids: list[str],
    policy_id: str | None,
    retention_days: int,
    trashed_at: datetime | None = None,
) -> int:
    if not gmail_message_ids:
        return 0

    trashed_at = trashed_at or _now()
    expiry_at = trashed_at + timedelta(days=int(retention_days))

    q = text(
        """
        UPDATE email_message
        SET
            lifecycle_state = 'TRASHED',
            trashed_at = :trashed_at,
            expiry_at = :expiry_at,
            trashed_by_policy_id = CASE
                WHEN :policy_id::text IS NULL THEN trashed_by_policy_id
                ELSE :policy_id::uuid
            END
        WHERE gmail_message_id = ANY(:ids)
          AND lifecycle_state <> 'TRASHED'
        """
    )

    with engine.begin() as conn:
        res = conn.execute(
            q,
            {
                "ids": gmail_message_ids,
                "policy_id": policy_id,
                "trashed_at": trashed_at,
                "expiry_at": expiry_at,
            },
        )
        return int(res.rowcount or 0)


def mark_messages_untrashed(
    engine,
    *,
    gmail_message_ids: list[str],
) -> int:
    if not gmail_message_ids:
        return 0

    q = text(
        """
        UPDATE email_message
        SET
            lifecycle_state = 'ACTIVE',
            trashed_at = NULL,
            expiry_at = NULL,
            trashed_by_policy_id = NULL
        WHERE gmail_message_id = ANY(:ids)
          AND lifecycle_state = 'TRASHED'
        """
    )

    with engine.begin() as conn:
        res = conn.execute(q, {"ids": gmail_message_ids})
        return int(res.rowcount or 0)


def mark_messages_expired(engine, *, gmail_message_ids: list[str]) -> int:
    if not gmail_message_ids:
        return 0

    q = text(
        """
        UPDATE email_message
        SET lifecycle_state = 'EXPIRED'
        WHERE gmail_message_id = ANY(:ids)
          AND lifecycle_state = 'TRASHED'
        """
    )

    with engine.begin() as conn:
        res = conn.execute(q, {"ids": gmail_message_ids})
        return int(res.rowcount or 0)


def list_expired_candidates(engine, *, limit: int = 500) -> list[dict]:
    """Return TRASHED messages whose expiry_at has passed.

    We require trashed_by_policy_id to be present for hard-delete eligibility.
    """

    q = text(
        """
        SELECT
            gmail_message_id,
            from_domain,
            subject,
            internal_date,
            trashed_at,
            expiry_at,
            trashed_by_policy_id::text
        FROM email_message
        WHERE lifecycle_state = 'TRASHED'
          AND expiry_at IS NOT NULL
          AND expiry_at <= NOW()
          AND trashed_by_policy_id IS NOT NULL
        ORDER BY expiry_at ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"limit": limit}).fetchall()

    return [
        {
            "gmail_message_id": r[0],
            "from_domain": r[1],
            "subject": r[2],
            "internal_date": r[3],
            "trashed_at": r[4],
            "expiry_at": r[5],
            "policy_id": r[6],
        }
        for r in rows
    ]


def log_action(
    engine,
    *,
    action_type: str,
    gmail_message_id: str | None,
    policy_id: str | None,
    status: str = "succeeded",
    error: str | None = None,
    from_domain: str | None = None,
    subject: str | None = None,
    internal_date: datetime | None = None,
    from_state: str | None = None,
    to_state: str | None = None,
    trashed_at: datetime | None = None,
    expiry_at: datetime | None = None,
) -> None:
    q = text(
        """
        INSERT INTO email_action_log (
            id,
            action_type,
            gmail_message_id,
            policy_id,
            status,
            error,
            occurred_at,
            from_domain,
            subject,
            internal_date,
            from_state,
            to_state,
            trashed_at,
            expiry_at
        )
        VALUES (
            :id::uuid,
            :action_type,
            :gmail_message_id,
            :policy_id::uuid,
            :status,
            :error,
            :occurred_at,
            :from_domain,
            :subject,
            :internal_date,
            :from_state,
            :to_state,
            :trashed_at,
            :expiry_at
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "id": str(uuid.uuid4()),
                "action_type": action_type,
                "gmail_message_id": gmail_message_id,
                "policy_id": policy_id,
                "status": status,
                "error": error,
                "occurred_at": _now(),
                "from_domain": from_domain,
                "subject": subject,
                "internal_date": internal_date,
                "from_state": from_state,
                "to_state": to_state,
                "trashed_at": trashed_at,
                "expiry_at": expiry_at,
            },
        )
