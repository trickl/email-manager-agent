"""SQLite-backed index for email header metadata.

The index is designed to store large numbers of email records (tens of
thousands+) without storing bodies, enabling fast search and aggregate analysis.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import structlog

from email_manager_agent.models import EmailHeader

logger = structlog.get_logger()


_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class EmailIndexStats:
    """High-level summary stats for the index."""

    total_messages: int
    unread_messages: int
    unique_senders: int
    min_date: datetime | None
    max_date: datetime | None


@dataclass(frozen=True)
class SenderStats:
    """Aggregate stats for a single sender."""

    from_email: str
    total_messages: int
    unread_messages: int


class EmailIndexRepository:
    """Repository for storing and querying email header metadata."""

    def __init__(self, db_path: Path) -> None:
        """Create a repository.

        Args:
            db_path: Path to the SQLite database file.
        """

        self._db_path = db_path

    def initialize(self) -> None:
        """Create or upgrade the index schema."""

        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

            current_version = self._get_schema_version(conn)
            if current_version is None:
                self._create_schema_v1(conn)
                self._set_schema_version(conn, _SCHEMA_VERSION)
                conn.commit()
                logger.info("email_index_schema_created", version=_SCHEMA_VERSION)
                return

            if current_version != _SCHEMA_VERSION:
                raise RuntimeError(
                    f"Unsupported schema version {current_version}; expected {_SCHEMA_VERSION}"
                )

    def upsert_many(self, headers: list[EmailHeader]) -> None:
        """Upsert a batch of email header records."""

        if not headers:
            return

        now_iso = datetime.now(timezone.utc).isoformat()

        with self._connect() as conn:
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.executemany(
                """
                INSERT INTO email_headers (
                    gmail_id,
                    thread_id,
                    internal_date_ms,
                    subject,
                    from_raw,
                    from_email,
                    to_addrs_json,
                    cc_addrs_json,
                    bcc_addrs_json,
                    reply_to,
                    date_iso,
                    label_ids_json,
                    is_unread,
                    is_inbox,
                    is_starred,
                    updated_at_iso
                )
                VALUES (
                    :gmail_id,
                    :thread_id,
                    :internal_date_ms,
                    :subject,
                    :from_raw,
                    :from_email,
                    :to_addrs_json,
                    :cc_addrs_json,
                    :bcc_addrs_json,
                    :reply_to,
                    :date_iso,
                    :label_ids_json,
                    :is_unread,
                    :is_inbox,
                    :is_starred,
                    :updated_at_iso
                )
                ON CONFLICT(gmail_id) DO UPDATE SET
                    thread_id=excluded.thread_id,
                    internal_date_ms=excluded.internal_date_ms,
                    subject=excluded.subject,
                    from_raw=excluded.from_raw,
                    from_email=excluded.from_email,
                    to_addrs_json=excluded.to_addrs_json,
                    cc_addrs_json=excluded.cc_addrs_json,
                    bcc_addrs_json=excluded.bcc_addrs_json,
                    reply_to=excluded.reply_to,
                    date_iso=excluded.date_iso,
                    label_ids_json=excluded.label_ids_json,
                    is_unread=excluded.is_unread,
                    is_inbox=excluded.is_inbox,
                    is_starred=excluded.is_starred,
                    updated_at_iso=excluded.updated_at_iso
                """,
                [
                    {
                        "gmail_id": h.gmail_id,
                        "thread_id": h.thread_id,
                        "internal_date_ms": h.internal_date_ms,
                        "subject": h.subject,
                        "from_raw": h.from_raw,
                        "from_email": h.from_email,
                        "to_addrs_json": json.dumps(h.to_addrs),
                        "cc_addrs_json": json.dumps(h.cc_addrs),
                        "bcc_addrs_json": json.dumps(h.bcc_addrs),
                        "reply_to": h.reply_to,
                        "date_iso": h.date.isoformat() if h.date else None,
                        "label_ids_json": json.dumps(h.label_ids),
                        "is_unread": 1 if h.is_unread else 0,
                        "is_inbox": 1 if h.is_inbox else 0,
                        "is_starred": 1 if h.is_starred else 0,
                        "updated_at_iso": now_iso,
                    }
                    for h in headers
                ],
            )
            conn.commit()

    def search(self, query: str, limit: int = 50, offset: int = 0) -> list[EmailHeader]:
        """Search the index using SQLite FTS5.

        Args:
            query: FTS query.
            limit: Max results.
            offset: Offset.

        Returns:
            Matching EmailHeader rows (best-effort; list fields reconstructed).
        """

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    e.gmail_id,
                    e.thread_id,
                    e.internal_date_ms,
                    e.subject,
                    e.from_raw,
                    e.from_email,
                    e.to_addrs_json,
                    e.cc_addrs_json,
                    e.bcc_addrs_json,
                    e.reply_to,
                    e.date_iso,
                    e.label_ids_json,
                    e.is_unread,
                    e.is_inbox,
                    e.is_starred
                FROM email_headers_fts
                JOIN email_headers e ON e.rowid = email_headers_fts.rowid
                WHERE email_headers_fts MATCH ?
                ORDER BY bm25(email_headers_fts)
                LIMIT ? OFFSET ?;
                """,
                (query, limit, offset),
            ).fetchall()

        return [self._row_to_header(row) for row in rows]

    def overall_stats(self) -> EmailIndexStats:
        """Compute high-level index stats."""

        with self._connect() as conn:
            total, unread = conn.execute(
                """
                SELECT COUNT(*), SUM(is_unread)
                FROM email_headers;
                """
            ).fetchone()

            (unique_senders,) = conn.execute(
                """
                SELECT COUNT(DISTINCT COALESCE(from_email, ''))
                FROM email_headers;
                """
            ).fetchone()

            min_iso, max_iso = conn.execute(
                """
                SELECT MIN(date_iso), MAX(date_iso)
                FROM email_headers;
                """
            ).fetchone()

        min_dt = datetime.fromisoformat(min_iso) if min_iso else None
        max_dt = datetime.fromisoformat(max_iso) if max_iso else None

        return EmailIndexStats(
            total_messages=int(total or 0),
            unread_messages=int(unread or 0),
            unique_senders=int(unique_senders or 0),
            min_date=min_dt,
            max_date=max_dt,
        )

    def top_senders(self, limit: int = 25) -> list[SenderStats]:
        """Return top senders by message count."""

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    COALESCE(from_email, ''),
                    COUNT(*) AS total_messages,
                    SUM(is_unread) AS unread_messages
                FROM email_headers
                GROUP BY COALESCE(from_email, '')
                ORDER BY total_messages DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()

        return [
            SenderStats(
                from_email=row[0],
                total_messages=int(row[1] or 0),
                unread_messages=int(row[2] or 0),
            )
            for row in rows
            if row[0]
        ]

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.row_factory = sqlite3.Row
            yield conn
        finally:
            conn.close()

    def _get_schema_version(self, conn: sqlite3.Connection) -> int | None:
        row = conn.execute(
            "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            return None
        return int(row[0])

    def _set_schema_version(self, conn: sqlite3.Connection, version: int) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO _schema_meta(key, value) VALUES('schema_version', ?) ",
            (str(version),),
        )

    def _create_schema_v1(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS email_headers (
                rowid INTEGER PRIMARY KEY,
                gmail_id TEXT NOT NULL UNIQUE,
                thread_id TEXT,
                internal_date_ms INTEGER,
                subject TEXT,
                from_raw TEXT,
                from_email TEXT,
                to_addrs_json TEXT NOT NULL,
                cc_addrs_json TEXT NOT NULL,
                bcc_addrs_json TEXT NOT NULL,
                reply_to TEXT,
                date_iso TEXT,
                label_ids_json TEXT NOT NULL,
                is_unread INTEGER NOT NULL,
                is_inbox INTEGER NOT NULL,
                is_starred INTEGER NOT NULL,
                updated_at_iso TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_email_headers_from_email
                ON email_headers(from_email);

            CREATE INDEX IF NOT EXISTS idx_email_headers_internal_date
                ON email_headers(internal_date_ms);

            CREATE VIRTUAL TABLE IF NOT EXISTS email_headers_fts USING fts5(
                subject,
                from_raw,
                from_email,
                to_addrs_json,
                cc_addrs_json,
                bcc_addrs_json,
                content='email_headers',
                content_rowid='rowid'
            );

            CREATE TRIGGER IF NOT EXISTS email_headers_ai
            AFTER INSERT ON email_headers
            BEGIN
                INSERT INTO email_headers_fts(
                    rowid, subject, from_raw, from_email, to_addrs_json, cc_addrs_json, bcc_addrs_json
                ) VALUES (
                    new.rowid, new.subject, new.from_raw, new.from_email, new.to_addrs_json,
                    new.cc_addrs_json, new.bcc_addrs_json
                );
            END;

            CREATE TRIGGER IF NOT EXISTS email_headers_ad
            AFTER DELETE ON email_headers
            BEGIN
                INSERT INTO email_headers_fts(email_headers_fts, rowid, subject, from_raw, from_email,
                    to_addrs_json, cc_addrs_json, bcc_addrs_json)
                VALUES('delete', old.rowid, old.subject, old.from_raw, old.from_email, old.to_addrs_json,
                    old.cc_addrs_json, old.bcc_addrs_json);
            END;

            CREATE TRIGGER IF NOT EXISTS email_headers_au
            AFTER UPDATE ON email_headers
            BEGIN
                INSERT INTO email_headers_fts(email_headers_fts, rowid, subject, from_raw, from_email,
                    to_addrs_json, cc_addrs_json, bcc_addrs_json)
                VALUES('delete', old.rowid, old.subject, old.from_raw, old.from_email, old.to_addrs_json,
                    old.cc_addrs_json, old.bcc_addrs_json);

                INSERT INTO email_headers_fts(
                    rowid, subject, from_raw, from_email, to_addrs_json, cc_addrs_json, bcc_addrs_json
                ) VALUES (
                    new.rowid, new.subject, new.from_raw, new.from_email, new.to_addrs_json,
                    new.cc_addrs_json, new.bcc_addrs_json
                );
            END;
            """
        )

    def _row_to_header(self, row: sqlite3.Row) -> EmailHeader:
        date = datetime.fromisoformat(row["date_iso"]) if row["date_iso"] else None

        return EmailHeader(
            gmail_id=row["gmail_id"],
            thread_id=row["thread_id"],
            internal_date_ms=row["internal_date_ms"],
            subject=row["subject"] or "",
            from_raw=row["from_raw"],
            from_email=row["from_email"],
            to_addrs=json.loads(row["to_addrs_json"]),
            cc_addrs=json.loads(row["cc_addrs_json"]),
            bcc_addrs=json.loads(row["bcc_addrs_json"]),
            reply_to=row["reply_to"],
            date=date,
            label_ids=json.loads(row["label_ids_json"]),
            is_unread=bool(row["is_unread"]),
            is_inbox=bool(row["is_inbox"]),
            is_starred=bool(row["is_starred"]),
        )
