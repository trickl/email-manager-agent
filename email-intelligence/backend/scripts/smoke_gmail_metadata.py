"""Smoke test: Gmail metadata read (and optional ingest).

Purpose:
- Verify Gmail OAuth credentials/token wiring works.
- Read a handful of messages using metadata-only calls (Phase 1 rules).
- Optionally run a tiny ingestion into Postgres + Qdrant to validate persistence wiring.

Usage (from email-intelligence/backend):
- ./.venv/bin/python scripts/smoke_gmail_metadata.py --max-messages 5
- ./.venv/bin/python scripts/smoke_gmail_metadata.py --max-messages 5 --ingest

Notes:
- This uses Settings() which reads EMAIL_INTEL_* vars and the local backend .env file.
- Gmail OAuth will open a browser on first run and create/update token.json.
"""

from __future__ import annotations

# Ensure the backend package root (email-intelligence/backend) is on sys.path so
# `import app.*` works when this script is executed from other directories.
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import argparse

from app.db.postgres import engine
from app.gmail.client import get_gmail_service_from_files, get_message_metadata, iter_message_ids
from app.ingestion.metadata_ingestion import ingest_metadata
from app.settings import Settings


def _require_file(path: str, *, label: str) -> None:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Missing {label} file at '{path}'. "
            "Create it locally (do not commit) or update the corresponding EMAIL_INTEL_*_PATH."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test Gmail metadata read / tiny ingest")
    parser.add_argument("--max-messages", type=int, default=5)
    parser.add_argument(
        "--auth",
        choices=["local_server", "console"],
        default="local_server",
        help=(
            "OAuth mode. 'local_server' opens a browser callback flow (recommended). "
            "'console' prints a URL and asks for a code (headless environments)."
        ),
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="If set, ingest the messages into Postgres + Qdrant (metadata only).",
    )
    args = parser.parse_args()

    if args.max_messages <= 0:
        raise ValueError("--max-messages must be > 0")

    settings = Settings()

    # Validate OAuth input files exist up-front (token may not exist yet; that's okay).
    _require_file(settings.gmail_credentials_path, label="Gmail credentials")

    token_exists = Path(settings.gmail_token_path).exists()
    print("AUTH")
    print(f"credentials_path={settings.gmail_credentials_path}")
    print(f"token_path={settings.gmail_token_path} exists={token_exists}")
    if not token_exists:
        print(
            "token.json does not exist yet. That is normal on first run. "
            "The OAuth flow will run now and create it."
        )

    service = get_gmail_service_from_files(
        credentials_path=settings.gmail_credentials_path,
        token_path=settings.gmail_token_path,
        auth_mode=args.auth,
    )

    if args.ingest:
        result = ingest_metadata(
            engine=engine,
            service=service,
            user_id=settings.gmail_user_id,
            page_size=min(settings.gmail_page_size, args.max_messages),
            max_messages=args.max_messages,
        )
        print("INGEST RESULT")
        print(result)
        return 0

    print("GMAIL METADATA SAMPLE")
    print(f"user_id={settings.gmail_user_id} max_messages={args.max_messages}")

    ids = []
    for i, msg_id in enumerate(
        iter_message_ids(
            service,
            user_id=settings.gmail_user_id,
            page_size=min(settings.gmail_page_size, args.max_messages),
        )
    ):
        ids.append(msg_id)
        if i + 1 >= args.max_messages:
            break

    print(f"Fetched {len(ids)} message ids")

    for idx, msg_id in enumerate(ids, start=1):
        meta = get_message_metadata(service, message_id=msg_id, user_id=settings.gmail_user_id)
        subj = meta.subject or "(no subject)"
        unread = "UNREAD" if meta.is_unread else "read"
        print(
            f"{idx:02d}. {meta.internal_date.isoformat()} [{unread}] from={meta.from_address} subj={subj}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
