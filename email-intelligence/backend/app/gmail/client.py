"""Gmail API client.

Phase 1 (metadata ingestion) rules:
- Use users.messages.list (paged)
- For each id, call users.messages.get with:
  - format=metadata
  - metadataHeaders: From, To, Cc, Bcc, Subject, Date
- Do NOT fetch bodies in Phase 1.

Phase 2 (clustering/labeling) rules:
- Fetch bodies only for representative samples.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import getaddresses, parseaddr
from typing import Iterable

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


METADATA_HEADERS: tuple[str, ...] = ("From", "To", "Cc", "Bcc", "Subject", "Date")


@dataclass(frozen=True)
class GmailMessageMetadata:
    gmail_message_id: str
    thread_id: str | None
    subject: str | None
    from_address: str
    to_addresses: list[str]
    cc_addresses: list[str]
    bcc_addresses: list[str]
    is_unread: bool
    internal_date: datetime
    label_ids: list[str]


def get_gmail_service_from_files(
    *,
    credentials_path: str,
    token_path: str,
    scopes: list[str] | None = None,
    auth_mode: str = "local_server",
    allow_interactive: bool = True,
):
    """Create a Gmail API service using local OAuth files.

    This is intended for local/dev usage. For server deployments, you will likely want a
    different auth strategy.
    """

    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/gmail.readonly"]

    creds: Credentials | None = None
    try:
        creds = Credentials.from_authorized_user_file(token_path, scopes=scopes)
    except FileNotFoundError:
        creds = None

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        if not allow_interactive:
            raise RuntimeError(
                "Gmail OAuth token is missing/invalid and interactive auth is disabled. "
                "Run an ingestion job once to complete OAuth and create token.json."
            )

        # Interactive local auth flow (creates/updates token file)
        flow = InstalledAppFlow.from_client_secrets_file(credentials_path, scopes=scopes)
        if auth_mode not in {"local_server", "console"}:
            raise ValueError("auth_mode must be 'local_server' or 'console'")

        print(
            "Gmail OAuth required. "
            f"Starting interactive flow (mode={auth_mode}). "
            f"This will block until you complete auth. token_path={token_path}"
        )

        if auth_mode == "console":
            # Useful for headless environments; prints a URL and prompts for a code.
            creds = flow.run_console()
        else:
            # Preferred for local dev; starts a localhost callback server.
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

        print(f"Wrote Gmail token to {token_path}")

    return build("gmail", "v1", credentials=creds)


def iter_message_ids(
    service,
    *,
    user_id: str = "me",
    page_size: int = 500,
    q: str | None = None,
    include_spam_trash: bool = True,
) -> Iterable[str]:
    """Yield Gmail message IDs using users.messages.list paging."""

    page_token: str | None = None
    while True:
        req = (
            service.users()
            .messages()
            .list(
                userId=user_id,
                maxResults=page_size,
                q=q,
                includeSpamTrash=include_spam_trash,
                pageToken=page_token,
            )
        )
        resp = req.execute()

        for msg in resp.get("messages", []) or []:
            msg_id = msg.get("id")
            if msg_id:
                yield msg_id

        page_token = resp.get("nextPageToken")
        if not page_token:
            return


def _header_value(payload: dict, name: str) -> str | None:
    headers = payload.get("headers", []) or []
    for h in headers:
        if (h.get("name") or "").lower() == name.lower():
            return h.get("value")
    return None


def _parse_address_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [addr for _, addr in getaddresses([value]) if addr]


def get_message_metadata(
    service,
    *,
    message_id: str,
    user_id: str = "me",
) -> GmailMessageMetadata:
    """Fetch message metadata using users.messages.get(format=metadata)."""

    msg = (
        service.users()
        .messages()
        .get(
            userId=user_id,
            id=message_id,
            format="metadata",
            metadataHeaders=list(METADATA_HEADERS),
        )
        .execute()
    )

    payload = msg.get("payload", {}) or {}
    subject = _header_value(payload, "Subject")
    from_raw = _header_value(payload, "From") or ""
    _, from_addr = parseaddr(from_raw)
    to_list = _parse_address_list(_header_value(payload, "To"))
    cc_list = _parse_address_list(_header_value(payload, "Cc"))
    bcc_list = _parse_address_list(_header_value(payload, "Bcc"))

    label_ids = msg.get("labelIds", []) or []
    is_unread = "UNREAD" in set(label_ids)

    internal_ms = int(msg.get("internalDate") or 0)
    internal_dt = datetime.fromtimestamp(internal_ms / 1000.0, tz=timezone.utc)

    return GmailMessageMetadata(
        gmail_message_id=msg.get("id"),
        thread_id=msg.get("threadId"),
        subject=subject,
        from_address=from_addr,
        to_addresses=to_list,
        cc_addresses=cc_list,
        bcc_addresses=bcc_list,
        is_unread=is_unread,
        internal_date=internal_dt,
        label_ids=list(label_ids),
    )


def get_message_body_text(
    service,
    *,
    message_id: str,
    user_id: str = "me",
    max_chars: int = 20_000,
) -> str:
    """Fetch and extract a best-effort text body.

    This is only intended for Phase 2 representative sampling.
    """

    import base64

    msg = (
        service.users()
        .messages()
        .get(
            userId=user_id,
            id=message_id,
            format="full",
        )
        .execute()
    )

    def decode_b64(data: str) -> str:
        raw = base64.urlsafe_b64decode(data.encode("utf-8"))
        return raw.decode("utf-8", errors="replace")

    def walk_parts(part: dict) -> list[str]:
        mime = (part.get("mimeType") or "").lower()
        body = part.get("body", {}) or {}
        data = body.get("data")
        if data and mime.startswith("text/plain"):
            return [decode_b64(data)]

        texts: list[str] = []
        for p in part.get("parts", []) or []:
            texts.extend(walk_parts(p))
        return texts

    payload = msg.get("payload", {}) or {}
    texts = walk_parts(payload)
    if texts:
        combined = "\n\n".join(texts).strip()
        return combined[:max_chars]

    # Fallback: Gmail's snippet is short but better than nothing.
    snippet = (msg.get("snippet") or "").strip()
    return snippet[:max_chars]


def list_label_names(service, *, user_id: str = "me") -> dict[str, str]:
    """Return a mapping of Gmail label id -> label name.

    Gmail message metadata includes label IDs. To show "folders" in the UI, we need the
    human-friendly label names.
    """

    resp = service.users().labels().list(userId=user_id).execute()
    labels = resp.get("labels", []) or []

    out: dict[str, str] = {}
    for l in labels:
        lid = l.get("id")
        name = l.get("name")
        if lid and name:
            out[str(lid)] = str(name)
    return out
