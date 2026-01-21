"""One-time Gmail OAuth helper.

Use this script to (re)authorize Gmail with the required scopes and write/update
`token.json`.

Why this exists:
- Background jobs (e.g. archive push) must not block waiting for interactive OAuth.
- If your existing token was created with gmail.readonly, Gmail modify operations
  will fail with a 403 (insufficient scopes).

Typical usage:
- Run once locally with a browser available to grant gmail.modify.
- Then re-run the background job.

This script is intentionally simple and prints only non-sensitive metadata.
"""

from __future__ import annotations

import argparse

from app.gmail.client import (
    GMAIL_SCOPE_MODIFY,
    GMAIL_SCOPE_READONLY,
    get_gmail_service_from_files,
)
from app.settings import Settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Authorize Gmail and write token.json")
    parser.add_argument(
        "--scope",
        choices=["readonly", "modify"],
        default="modify",
        help="OAuth scope to authorize (default: modify)",
    )
    parser.add_argument(
        "--auth-mode",
        choices=["local_server", "console"],
        default=None,
        help="OAuth flow mode override (default: use Settings.gmail_auth_mode)",
    )

    args = parser.parse_args()

    settings = Settings()
    scope = GMAIL_SCOPE_MODIFY if args.scope == "modify" else GMAIL_SCOPE_READONLY
    auth_mode = args.auth_mode or settings.gmail_auth_mode

    # This call performs interactive OAuth if required and writes token.json.
    _ = get_gmail_service_from_files(
        credentials_path=settings.gmail_credentials_path,
        token_path=settings.gmail_token_path,
        scopes=[scope],
        auth_mode=auth_mode,
        allow_interactive=True,
    )

    print(
        "Gmail OAuth complete. "
        f"token_path={settings.gmail_token_path} "
        f"scope={args.scope} "
        f"auth_mode={auth_mode}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
