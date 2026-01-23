"""Google Calendar API client helpers.

We keep this separate from Gmail to allow different OAuth scopes and token files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


CALENDAR_SCOPE_EVENTS = "https://www.googleapis.com/auth/calendar.events"


def get_calendar_service_from_files(
    *,
    credentials_path: str,
    token_path: str,
    scopes: Sequence[str] | None = None,
    auth_mode: str = "local_server",
    allow_interactive: bool = True,
):
    """Create an authenticated Google Calendar API service.

    Args:
        credentials_path: Path to OAuth client credentials JSON.
        token_path: Path to token JSON used to store refresh token.
        scopes: OAuth scopes to request. Defaults to calendar.events.
        auth_mode: local_server|console
        allow_interactive: If False, will not trigger interactive OAuth flow.

    Returns:
        googleapiclient service for calendar v3.

    Raises:
        RuntimeError: If interactive auth is required but disabled.
    """

    scopes = list(scopes or [CALENDAR_SCOPE_EVENTS])

    cred_file = Path(credentials_path)
    token_file = Path(token_path)

    creds: Credentials | None = None
    if token_file.exists():
        creds = Credentials.from_authorized_user_file(str(token_file), scopes)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        if not allow_interactive:
            raise RuntimeError(
                "Calendar credentials are missing/invalid and interactive auth is disabled. "
                "Enable EMAIL_INTEL_CALENDAR_ALLOW_INTERACTIVE or provide a valid calendar token file."
            )

        flow = InstalledAppFlow.from_client_secrets_file(str(cred_file), scopes)

        if auth_mode == "console":
            creds = flow.run_console()
        else:
            # Default: local server flow.
            creds = flow.run_local_server(port=0)

        token_file.write_text(creds.to_json(), encoding="utf-8")

    return build("calendar", "v3", credentials=creds)
