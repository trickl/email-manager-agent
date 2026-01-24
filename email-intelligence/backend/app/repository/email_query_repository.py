"""Query helpers for clustering/labeling and observability."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.domain.email import EmailMessage


@dataclass(frozen=True)
class EmailRow:
    email: EmailMessage
    category: str | None
    cluster_id: str | None


def count_total(engine) -> int:
    from sqlalchemy import text

    q = text(
        """
        SELECT COUNT(*)
        FROM email_message
        WHERE NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        """
    )
    with engine.begin() as conn:
        return int(conn.execute(q).scalar() or 0)


def count_labelled(engine) -> int:
    from sqlalchemy import text

    q = text(
        """
        SELECT COUNT(*)
        FROM email_message
        WHERE category IS NOT NULL
          AND NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        """
    )
    with engine.begin() as conn:
        return int(conn.execute(q).scalar() or 0)


def count_unlabelled(engine) -> int:
    from sqlalchemy import text

    q = text(
        """
        SELECT COUNT(*)
        FROM email_message
        WHERE category IS NULL
          AND NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        """
    )
    with engine.begin() as conn:
        return int(conn.execute(q).scalar() or 0)


def count_unlabelled_since(engine, *, received_since: datetime) -> int:
    from sqlalchemy import text

    q = text(
        """
        SELECT COUNT(*)
        FROM email_message
        WHERE category IS NULL
          AND internal_date >= :received_since
          AND NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        """
    )
    with engine.begin() as conn:
        return int(conn.execute(q, {"received_since": received_since}).scalar() or 0)


def count_clusters(engine) -> int:
    from sqlalchemy import text

    q = text("SELECT COUNT(*) FROM email_cluster")
    with engine.begin() as conn:
        return int(conn.execute(q).scalar() or 0)


def _row_to_email(row) -> EmailMessage:
    return EmailMessage(
        gmail_message_id=row[0],
        thread_id=row[1],
        subject=row[2],
        subject_normalized=row[3],
        from_address=row[4],
        from_domain=row[5],
        to_addresses=list(row[6] or []),
        cc_addresses=list(row[7] or []),
        bcc_addresses=list(row[8] or []),
        is_unread=bool(row[9]),
        internal_date=row[10],
        label_ids=list(row[11] or []),
    )


def fetch_next_unlabelled(engine) -> EmailRow | None:
    """Return the next unlabelled email (deterministic order)."""

    from sqlalchemy import text

    q = text(
        """
        SELECT
            gmail_message_id,
            thread_id,
            subject,
            subject_normalized,
            from_address,
            from_domain,
            to_addresses,
            cc_addresses,
            bcc_addresses,
            is_unread,
            internal_date,
            label_ids,
            category,
            cluster_id
        FROM email_message
        WHERE category IS NULL
                    AND NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        ORDER BY internal_date ASC, gmail_message_id ASC
        LIMIT 1
        """
    )

    with engine.begin() as conn:
        row = conn.execute(q).fetchone()

    if row is None:
        return None

    email = _row_to_email(row)
    return EmailRow(email=email, category=row[12], cluster_id=row[13])


def fetch_next_unlabelled_since(
    engine,
    *,
    received_since: datetime,
) -> EmailRow | None:
    """Return the next unlabelled email since a given timestamp."""

    from sqlalchemy import text

    q = text(
        """
        SELECT
            gmail_message_id,
            thread_id,
            subject,
            subject_normalized,
            from_address,
            from_domain,
            to_addresses,
            cc_addresses,
            bcc_addresses,
            is_unread,
            internal_date,
            label_ids,
            category,
            cluster_id
        FROM email_message
        WHERE category IS NULL
          AND internal_date >= :received_since
          AND NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        ORDER BY internal_date ASC, gmail_message_id ASC
        LIMIT 1
        """
    )

    with engine.begin() as conn:
        row = conn.execute(q, {"received_since": received_since}).fetchone()

    if row is None:
        return None

    email = _row_to_email(row)
    return EmailRow(email=email, category=row[12], cluster_id=row[13])


def fetch_by_gmail_ids(engine, gmail_ids: list[str]) -> list[EmailRow]:
    if not gmail_ids:
        return []

    from sqlalchemy import text

    q = text(
        """
        SELECT
            gmail_message_id,
            thread_id,
            subject,
            subject_normalized,
            from_address,
            from_domain,
            to_addresses,
            cc_addresses,
            bcc_addresses,
            is_unread,
            internal_date,
            label_ids,
            category,
            cluster_id
        FROM email_message
        WHERE gmail_message_id = ANY(:gmail_ids)
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"gmail_ids": gmail_ids}).fetchall()

    result: list[EmailRow] = []
    for row in rows:
        result.append(EmailRow(email=_row_to_email(row), category=row[12], cluster_id=row[13]))
    return result


def fetch_unlabelled_by_domain(engine, *, from_domain: str, limit: int = 2000) -> list[EmailRow]:
    """Fetch unlabelled emails for a given sender domain.

    This is used as a cheap, deterministic candidate set for clustering when we don't have
    meaningful semantic embeddings.

    Args:
        engine: SQLAlchemy engine.
        from_domain: Sender domain to filter by.
        limit: Maximum number of rows to return.

    Returns:
        A list of EmailRow values ordered by internal_date asc.
    """

    from sqlalchemy import text

    q = text(
        """
        SELECT
            gmail_message_id,
            thread_id,
            subject,
            subject_normalized,
            from_address,
            from_domain,
            to_addresses,
            cc_addresses,
            bcc_addresses,
            is_unread,
            internal_date,
            label_ids,
            category,
            cluster_id
        FROM email_message
        WHERE category IS NULL
          AND from_domain = :from_domain
                    AND NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        ORDER BY internal_date ASC, gmail_message_id ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"from_domain": from_domain, "limit": int(limit)}).fetchall()

    result: list[EmailRow] = []
    for row in rows:
        result.append(EmailRow(email=_row_to_email(row), category=row[12], cluster_id=row[13]))
    return result


def insert_cluster(
    *,
    engine,
    cluster_id: str,
    seed_gmail_message_id: str,
    from_domain: str,
    subject_normalized: str | None,
    similarity_threshold: float,
    display_name: str | None = None,
) -> None:
    from sqlalchemy import text

    q = text(
        """
        INSERT INTO email_cluster (
            id,
            seed_gmail_message_id,
            from_domain,
            subject_normalized,
            similarity_threshold,
            display_name
        )
        VALUES (
            :id,
            :seed_gmail_message_id,
            :from_domain,
            :subject_normalized,
            :similarity_threshold,
            :display_name
        )
        ON CONFLICT (seed_gmail_message_id) DO NOTHING
        """
    )

    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "id": cluster_id,
                "seed_gmail_message_id": seed_gmail_message_id,
                "from_domain": from_domain,
                "subject_normalized": subject_normalized,
                "similarity_threshold": similarity_threshold,
                "display_name": display_name,
            },
        )


def update_cluster_analysis(
    *,
    engine,
    cluster_id: str,
    frequency_label: str,
    unread_label: str,
) -> None:
    from sqlalchemy import text

    q = text(
        """
        UPDATE email_cluster
        SET
            frequency_label = :frequency_label,
            unread_label = :unread_label
        WHERE id = :id
        """
    )

    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "id": cluster_id,
                "frequency_label": frequency_label,
                "unread_label": unread_label,
            },
        )


def update_cluster_label(
    *,
    engine,
    cluster_id: str,
    category: str,
    subcategory: str | None,
    label_version: str,
) -> None:
    from sqlalchemy import text

    q = text(
        """
        UPDATE email_cluster
        SET
            category = :category,
            subcategory = :subcategory,
            label_confidence = NULL,
            label_version = :label_version
        WHERE id = :id
        """
    )

    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "id": cluster_id,
                "category": category,
                "subcategory": subcategory,
                "label_version": label_version,
            },
        )


def label_emails_in_cluster(
    *,
    engine,
    gmail_ids: list[str],
    cluster_id: str,
    category: str,
    subcategory: str | None,
    label_version: str,
) -> int:
    """Label emails that are currently unlabelled.

    Returns:
        Number of rows updated.
    """

    if not gmail_ids:
        return 0

    from sqlalchemy import text

    q = text(
        """
        UPDATE email_message
        SET
            category = :category,
            subcategory = :subcategory,
                        label_confidence = NULL,
            label_version = :label_version,
            cluster_id = :cluster_id
        WHERE gmail_message_id = ANY(:gmail_ids)
          AND category IS NULL
        """
    )

    with engine.begin() as conn:
        res = conn.execute(
            q,
            {
                "gmail_ids": gmail_ids,
                "category": category,
                "subcategory": subcategory,
                "label_version": label_version,
                "cluster_id": cluster_id,
            },
        )
        return int(res.rowcount or 0)


def latest_internal_date(engine) -> datetime | None:
    from sqlalchemy import text

    q = text("SELECT MAX(internal_date) FROM email_message")
    with engine.begin() as conn:
        return conn.execute(q).scalar()


def fetch_recent_domain_activity(
    engine,
    *,
    from_domain: str,
    limit: int = 30,
) -> tuple[list[datetime], list[bool]]:
    """Fetch recent timestamps and unread flags for a sender domain.

    This is used to provide lightweight context for incremental, per-email labeling.
    We intentionally do NOT fetch bodies here.

    Args:
        engine: SQLAlchemy engine.
        from_domain: Sender domain to filter by.
        limit: Max number of rows considered.

    Returns:
        (dates, is_unread_flags) ordered oldest -> newest.
    """

    from sqlalchemy import text

    limit = max(1, min(int(limit), 200))

    q = text(
        """
        SELECT internal_date, is_unread
        FROM email_message
        WHERE from_domain = :from_domain
        ORDER BY internal_date DESC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q, {"from_domain": from_domain, "limit": limit}).fetchall()

    dates: list[datetime] = []
    unread: list[bool] = []
    for d, u in rows:
        if d is None:
            continue
        dates.append(d)
        unread.append(bool(u))

    # We queried DESC for recency; flip to ASC for frequency_label().
    dates.reverse()
    unread.reverse()
    return dates, unread
