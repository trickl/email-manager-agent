"""Batch payment extraction for Financial emails.

Usage (from email-intelligence/backend):
  ./.venv/bin/python scripts/run_payment_extraction_financial.py --limit 500

Notes:
- Requires Postgres (EMAIL_INTEL_DATABASE_URL) and Gmail OAuth files.
- Requires Ollama configured (EMAIL_INTEL_OLLAMA_HOST).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure backend root is on sys.path so `import app.*` works.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.analysis.payments.extractor import extract_payment_from_email
from app.analysis.payments.prompt import PROMPT_VERSION
from app.db.schema import ensure_core_schema
from app.db.postgres import engine
from app.gmail.client import get_gmail_service_from_files, get_message_body_text
from app.repository.payment_metadata_repository import (
    list_messages_in_category_any_subcategory,
    upsert_message_payment_metadata,
)
from app.settings import Settings


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract payment metadata from Financial emails")
    p.add_argument("--limit", type=int, default=500)
    p.add_argument(
        "--max-body-chars",
        type=int,
        default=30_000,
        help="Max body characters to fetch per message",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    settings = Settings()

    ensure_core_schema(engine)

    if not settings.ollama_host:
        raise RuntimeError(
            "Ollama is not configured. Set EMAIL_INTEL_OLLAMA_HOST (e.g. http://localhost:11434)."
        )

    limit = max(1, min(int(args.limit), 5000))

    service = get_gmail_service_from_files(
        credentials_path=settings.gmail_credentials_path,
        token_path=settings.gmail_token_path,
        auth_mode=settings.gmail_auth_mode,
        allow_interactive=settings.gmail_allow_interactive,
    )

    rows = list_messages_in_category_any_subcategory(
        engine=engine,
        category="Financial",
        limit=limit,
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
            extracted = extract_payment_from_email(
                ollama_host=settings.ollama_host,
                ollama_model=settings.ollama_model,
                subject=str(subj) if subj is not None else None,
                from_domain=str(from_domain) if from_domain is not None else None,
                internal_date_iso=internal_iso,
                body=body,
            )

            if extracted.cost_amount or extracted.vendor_name or extracted.item_name:
                status = "succeeded"
            else:
                status = "no_payment"

            was_insert = upsert_message_payment_metadata(
                engine=engine,
                message_id=mid,
                status=status,
                error=None,
                item_name=extracted.item_name,
                vendor_name=extracted.vendor_name,
                item_category=extracted.item_category,
                cost_amount=extracted.cost_amount,
                cost_currency=extracted.cost_currency,
                is_recurring=extracted.is_recurring,
                frequency=extracted.frequency,
                payment_date=extracted.payment_date,
                payment_fingerprint=extracted.payment_fingerprint,
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

            if i % 10 == 0 or i == total:
                print(
                    f"progress processed={i}/{total} inserted={inserted} updated={updated} failed={failed}"
                )
        except Exception as e:  # noqa: BLE001
            failed += 1
            upsert_message_payment_metadata(
                engine=engine,
                message_id=mid,
                status="failed",
                error=str(e),
                item_name=None,
                vendor_name=None,
                item_category=None,
                cost_amount=None,
                cost_currency=None,
                is_recurring=None,
                frequency=None,
                payment_date=None,
                payment_fingerprint=None,
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
