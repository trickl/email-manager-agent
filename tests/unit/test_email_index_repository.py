"""Unit tests for the header-only email index repository."""

from __future__ import annotations

from datetime import datetime, timezone

from email_manager_agent.index import EmailIndexRepository
from email_manager_agent.models import EmailHeader


def test_repository_initialize_and_upsert_search(tmp_path) -> None:
    db_path = tmp_path / "index.sqlite3"
    repo = EmailIndexRepository(db_path)
    repo.initialize()

    h1 = EmailHeader(
        gmail_id="m1",
        thread_id="t1",
        internal_date_ms=1700000000000,
        subject="Welcome to Python Weekly",
        from_raw="Python Weekly <newsletter@python.org>",
        from_email="newsletter@python.org",
        to_addrs=["me@example.com"],
        cc_addrs=[],
        bcc_addrs=[],
        reply_to=None,
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        label_ids=["INBOX", "UNREAD"],
        is_unread=True,
        is_inbox=True,
        is_starred=False,
    )
    h2 = EmailHeader(
        gmail_id="m2",
        thread_id="t2",
        subject="Your invoice is ready",
        from_raw="Billing <billing@example.com>",
        from_email="billing@example.com",
        to_addrs=["me@example.com"],
        label_ids=["INBOX"],
        is_unread=False,
        is_inbox=True,
        is_starred=False,
    )

    repo.upsert_many([h1, h2])

    results = repo.search("Python")
    assert len(results) == 1
    assert results[0].gmail_id == "m1"


def test_repository_stats(tmp_path) -> None:
    db_path = tmp_path / "index.sqlite3"
    repo = EmailIndexRepository(db_path)
    repo.initialize()

    repo.upsert_many(
        [
            EmailHeader(
                gmail_id="m1",
                subject="Sale now on",
                from_email="shop@example.com",
                label_ids=["UNREAD"],
                is_unread=True,
            ),
            EmailHeader(
                gmail_id="m2",
                subject="Sale ends soon",
                from_email="shop@example.com",
                label_ids=[],
                is_unread=False,
            ),
            EmailHeader(
                gmail_id="m3",
                subject="Meeting notes",
                from_email="colleague@example.com",
                label_ids=["UNREAD"],
                is_unread=True,
            ),
        ]
    )

    stats = repo.overall_stats()
    assert stats.total_messages == 3
    assert stats.unread_messages == 2
    assert stats.unique_senders >= 2

    top = repo.top_senders(limit=10)
    assert top[0].from_email == "shop@example.com"
    assert top[0].total_messages == 2
