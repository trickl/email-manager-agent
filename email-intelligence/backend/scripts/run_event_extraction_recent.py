"""Batch event extraction for recent Gmail mail.

This script:
1) Lists Gmail message ids newer than N days.
2) Upserts their metadata into Postgres (email_message).
3) Runs event extraction (Ollama) and stores results in message_event_metadata.

Usage (from email-intelligence/backend):
  ./.venv/bin/python scripts/run_event_extraction_recent.py --days 14

Notes:
- Requires Postgres (EMAIL_INTEL_DATABASE_URL) and Gmail OAuth files.
- Requires Ollama configured (EMAIL_INTEL_OLLAMA_HOST).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure backend root is on sys.path so `import app.*` works.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.analysis.events.extractor import extract_event_from_email
from app.analysis.events.prompt import PROMPT_VERSION
from app.db.schema import ensure_core_schema
from app.db.postgres import engine
from app.gmail.client import get_gmail_service_from_files, get_message_body_text, get_message_metadata, iter_message_ids
from app.gmail.mapping import metadata_to_domain
from app.repository.email_repository import insert_email
from app.repository.event_metadata_repository import list_messages_received_since, upsert_message_event_metadata
from app.settings import Settings


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract event metadata from recent emails")
    p.add_argument("--days", type=int, default=14, help="Only process messages newer than this many days")
    p.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Optional cap on the number of Gmail messages to scan (for safety)",
    )
    p.add_argument(
        "--include-trash",
        action="store_true",
        help="Include messages in TRASH when selecting from Postgres",
    )
    p.add_argument(
        "--max-body-chars",
        type=int,
        default=30_000,
        help="Max body characters to fetch per message",
    )
    return p.parse_args()


def _cutoff_utc(days: int) -> datetime:
    days = max(1, int(days))
    return datetime.now(timezone.utc) - timedelta(days=days)


def main() -> int:
    args = _parse_args()
    settings = Settings()

    # Ensure schema exists for existing DB volumes.
    ensure_core_schema(engine)

    if not settings.ollama_host:
        raise RuntimeError(
            "Ollama is not configured. Set EMAIL_INTEL_OLLAMA_HOST (e.g. http://localhost:11434)."
        )

    cutoff = _cutoff_utc(args.days)
    cutoff_q = f"after:{int((cutoff - timedelta(seconds=1)).timestamp())}"

    service = get_gmail_service_from_files(
        credentials_path=settings.gmail_credentials_path,
        token_path=settings.gmail_token_path,
        auth_mode=settings.gmail_auth_mode,
        allow_interactive=settings.gmail_allow_interactive,
    )

    # Step 1: Ensure recent messages exist in Postgres.
    scanned = 0
    inserted_meta = 0
    failed_meta = 0

    print(f"sync_metadata_start cutoff={cutoff.isoformat()} gmail_query={cutoff_q}")

    for msg_id in iter_message_ids(service, user_id=settings.gmail_user_id, page_size=500, q=cutoff_q):
        if args.max_messages is not None and scanned >= int(args.max_messages):
            break

        scanned += 1
        try:
            meta = get_message_metadata(service, message_id=msg_id, user_id=settings.gmail_user_id)
            email = metadata_to_domain(meta)

            # Extra defense: Gmail search is best-effort; enforce cutoff.
            if email.internal_date.tzinfo is None:
                # Treat naive as UTC.
                email.internal_date = email.internal_date.replace(tzinfo=timezone.utc)
            if email.internal_date < cutoff:
                continue

            insert_email(email)
            inserted_meta += 1

            if scanned % 250 == 0:
                print(
                    f"sync_metadata_progress scanned={scanned} upserted={inserted_meta} failed={failed_meta}"
                )
        except Exception as e:  # noqa: BLE001
            failed_meta += 1
            print(f"sync_metadata_failed scanned={scanned} gmail_message_id={msg_id} error={e}")

    print(
        f"sync_metadata_done scanned={scanned} upserted={inserted_meta} failed={failed_meta} cutoff={cutoff.isoformat()}"
    )

    # Step 2: Extract events for the recent DB slice.
    rows = list_messages_received_since(
        engine=engine,
        received_since=cutoff,
        limit=50_000,
        include_trash=bool(args.include_trash),
    )

    total = len(rows)
    print(f"loaded_messages total={total}")

    inserted = 0
    updated = 0
    failed = 0

    for i, r in enumerate(rows, start=1):
        mid = int(r["message_id"])
        gid = str(r["gmail_message_id"])
        subj = r.get("subject")
        from_domain = r.get("from_domain")
        internal_date = r.get("internal_date")
        internal_iso = internal_date.isoformat() if internal_date is not None else None

        try:
            body = get_message_body_text(
                service,
                message_id=gid,
                user_id=settings.gmail_user_id,
                max_chars=int(args.max_body_chars),
            )
            extracted = extract_event_from_email(
                ollama_host=settings.ollama_host,
                ollama_model=settings.ollama_model,
                subject=str(subj) if subj is not None else None,
                from_domain=str(from_domain) if from_domain is not None else None,
                internal_date_iso=internal_iso,
                body=body,
            )

            if extracted.event_name or extracted.event_date or extracted.start_time:
                status = "succeeded"
            else:
                status = "no_event"

            was_insert = upsert_message_event_metadata(
                engine=engine,
                message_id=mid,
                status=status,
                error=None,
                event_name=extracted.event_name,
                event_type=extracted.event_type,
                event_date=extracted.event_date,
                start_time=extracted.start_time,
                end_time=extracted.end_time,
                timezone=extracted.timezone,
                end_time_inferred=bool(extracted.end_time_inferred),
                confidence=extracted.confidence,
                model=extracted.model,
                prompt_version=extracted.prompt_version,
                raw_json=extracted.raw_json,
                extracted_at=datetime.now(timezone.utc),
            )

            if was_insert:
                inserted += 1
            else:
                updated += 1

            if i % 25 == 0 or i == total:
                print(
                    f"progress processed={i}/{total} inserted={inserted} updated={updated} failed={failed}"
                )
        except Exception as e:  # noqa: BLE001
            failed += 1
            upsert_message_event_metadata(
                engine=engine,
                message_id=mid,
                status="failed",
                error=str(e),
                event_name=None,
                event_type=None,
                event_date=None,
                start_time=None,
                end_time=None,
                timezone=None,
                end_time_inferred=False,
                confidence=None,
                model=settings.ollama_model,
                prompt_version=PROMPT_VERSION,
                raw_json=None,
                extracted_at=datetime.now(timezone.utc),
            )
            print(f"failed processed={i}/{total} gmail_message_id={gid} error={e}")

    print(f"done total={total} inserted={inserted} updated={updated} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
