"""Run the full maintenance pipeline.

Usage (from email-intelligence/backend):
  ./.venv/bin/python scripts/run_maintenance.py

Notes:
- Requires Postgres (EMAIL_INTEL_DATABASE_URL) and Gmail OAuth files.
- Requires Ollama configured for extraction steps (EMAIL_INTEL_OLLAMA_HOST).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure backend root is on sys.path so `import app.*` works.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.schema import ensure_core_schema
from app.db.postgres import engine
from app.maintenance import run_maintenance
from app.settings import Settings


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the Email Intelligence maintenance pipeline")
    p.add_argument("--inbox-cleanup-days", type=int, default=None)
    p.add_argument("--label-threshold", type=int, default=None)
    p.add_argument("--fallback-days", type=int, default=None)
    p.add_argument(
        "--allow-interactive",
        action="store_true",
        help="Allow interactive OAuth flows if tokens are missing",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    settings = Settings()

    ensure_core_schema(engine)

    def progress_cb(
        *,
        phase: str,
        message: str | None = None,
        processed: int | None = None,
        total: int | None = None,
        inserted: int | None = None,
        skipped_existing: int | None = None,
        failed: int | None = None,
    ) -> None:
        parts = [f"phase={phase}"]
        if message:
            parts.append(f"message={message}")
        if total is not None:
            parts.append(f"total={total}")
        if processed is not None:
            parts.append(f"processed={processed}")
        if inserted is not None:
            parts.append(f"inserted={inserted}")
        if skipped_existing is not None:
            parts.append(f"skipped_existing={skipped_existing}")
        if failed is not None:
            parts.append(f"failed={failed}")
        print(" ".join(parts))

    run_maintenance(
        engine=engine,
        settings=settings,
        inbox_cleanup_days=args.inbox_cleanup_days,
        label_threshold=args.label_threshold,
        fallback_days=args.fallback_days,
        allow_interactive=bool(args.allow_interactive),
        progress_cb=progress_cb,
    )

    print("maintenance_complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
