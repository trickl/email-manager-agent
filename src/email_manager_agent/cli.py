"""Command-line interface for Email Manager Agent.

This module provides the main entry point for the CLI application.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import structlog

from email_manager_agent.config import get_settings
from email_manager_agent.gmail.client import GmailClient
from email_manager_agent.gmail.parsing import message_to_email_header
from email_manager_agent.index import EmailIndexRepository

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="email-manager", description="Email Manager Agent")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Index commands
    index_parser = subparsers.add_parser(
        "index",
        help="Build and query a local header-only email index",
    )
    index_sub = index_parser.add_subparsers(dest="index_command", required=True)

    build_parser = index_sub.add_parser("build", help="Download Gmail headers and store locally")
    build_parser.add_argument(
        "--query",
        default=None,
        help="Optional Gmail search query (same syntax as Gmail search box)",
    )
    build_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of messages to index (default: all)",
    )
    build_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to the SQLite index database (default: settings index_db_path)",
    )
    build_parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Upsert batch size (default: settings index_batch_size)",
    )

    search_parser = index_sub.add_parser("search", help="Search the local header index")
    search_parser.add_argument("query", help="FTS query")
    search_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to the SQLite index database (default: settings index_db_path)",
    )
    search_parser.add_argument("--limit", type=int, default=25, help="Max results")

    stats_parser = index_sub.add_parser("stats", help="Show index stats and top senders")
    stats_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to the SQLite index database (default: settings index_db_path)",
    )
    stats_parser.add_argument("--top-senders", type=int, default=25, help="Number of top senders")

    return parser


async def _cmd_index_build(args: argparse.Namespace) -> int:
    settings = get_settings()
    db_path: Path = args.db or settings.index_db_path
    batch_size: int = args.batch_size or settings.index_batch_size

    repo = EmailIndexRepository(db_path)
    repo.initialize()

    gmail = GmailClient(settings)
    await gmail.authenticate()

    # Metadata-only fetch: no body.
    metadata_headers = [
        "Subject",
        "From",
        "To",
        "Cc",
        "Bcc",
        "Date",
        "Reply-To",
        "List-Unsubscribe",
        "Message-ID",
    ]

    messages = await gmail.list_messages(max_results=args.limit, query=args.query)
    logger.info(
        "index_build_list_complete",
        message_count=len(messages),
        query=args.query,
        limit=args.limit,
    )

    batch = []
    processed = 0
    for m in messages:
        message_id = m.get("id")
        if not isinstance(message_id, str) or not message_id:
            continue

        raw = await gmail.get_message(message_id, format="metadata", metadata_headers=metadata_headers)
        header = message_to_email_header(raw)
        if not header.gmail_id:
            continue

        batch.append(header)
        processed += 1

        if len(batch) >= batch_size:
            repo.upsert_many(batch)
            logger.info("index_batch_upserted", batch_size=len(batch), processed=processed)
            batch.clear()

    if batch:
        repo.upsert_many(batch)
        logger.info("index_batch_upserted", batch_size=len(batch), processed=processed)

    stats = repo.overall_stats()
    print(
        f"Indexed {stats.total_messages} messages ({stats.unread_messages} unread) "
        f"from {stats.unique_senders} senders into {db_path}"
    )
    return 0


def _cmd_index_search(args: argparse.Namespace) -> int:
    settings = get_settings()
    db_path: Path = args.db or settings.index_db_path
    repo = EmailIndexRepository(db_path)
    repo.initialize()

    results = repo.search(args.query, limit=args.limit)
    for h in results:
        unread = "UNREAD" if h.is_unread else "READ"
        from_part = h.from_email or h.from_raw or "(unknown sender)"
        date_part = h.date.isoformat() if h.date else "(no date)"
        print(f"{unread}\t{date_part}\t{from_part}\t{h.subject}")

    return 0


def _cmd_index_stats(args: argparse.Namespace) -> int:
    settings = get_settings()
    db_path: Path = args.db or settings.index_db_path
    repo = EmailIndexRepository(db_path)
    repo.initialize()

    stats = repo.overall_stats()
    print(f"Total messages: {stats.total_messages}")
    print(f"Unread messages: {stats.unread_messages}")
    print(f"Unique senders: {stats.unique_senders}")
    if stats.min_date and stats.max_date:
        print(f"Date range: {stats.min_date.date().isoformat()} -> {stats.max_date.date().isoformat()}")

    print("\nTop senders:")
    for s in repo.top_senders(limit=args.top_senders):
        unread_rate = 0.0 if s.total_messages == 0 else s.unread_messages / s.total_messages
        print(f"- {s.from_email}: {s.total_messages} messages ({unread_rate:.0%} unread)")

    return 0


def main(args: list[str] | None = None) -> int:
    """Main entry point for the Email Manager Agent CLI.

    Args:
        args: Command-line arguments. If None, uses sys.argv.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    if args is None:
        args = sys.argv[1:]

    settings = get_settings()

    # Configure logging
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(structlog.stdlib, settings.log_level)
        ),
    )

    logger.info("email_manager_agent_started", version="0.1.0", debug=settings.debug)

    parser = _build_parser()
    parsed = parser.parse_args(args)

    if parsed.command == "index":
        if parsed.index_command == "build":
            return asyncio.run(_cmd_index_build(parsed))
        if parsed.index_command == "search":
            return _cmd_index_search(parsed)
        if parsed.index_command == "stats":
            return _cmd_index_stats(parsed)

    logger.error("unknown_command", command=parsed.command)
    return 2


if __name__ == "__main__":
    sys.exit(main())
