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

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

import json


METADATA_HEADERS: tuple[str, ...] = ("From", "To", "Cc", "Bcc", "Subject", "Date")

GMAIL_SCOPE_READONLY = "https://www.googleapis.com/auth/gmail.readonly"
GMAIL_SCOPE_MODIFY = "https://www.googleapis.com/auth/gmail.modify"


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
        scopes = [GMAIL_SCOPE_READONLY]

    missing_scopes: set[str] | None = None

    token_file_scopes: set[str] | None = None
    try:
        with open(token_path, "r", encoding="utf-8") as f:
            token_obj = json.load(f)
        raw = token_obj.get("scopes")
        if isinstance(raw, list):
            token_file_scopes = {str(s) for s in raw if s}
    except FileNotFoundError:
        token_file_scopes = None
    except Exception:
        # If the token exists but we can't parse it, let the normal credential load fail
        # and fall back to interactive auth if allowed.
        token_file_scopes = None

    creds: Credentials | None = None
    try:
        creds = Credentials.from_authorized_user_file(token_path, scopes=scopes)
    except FileNotFoundError:
        creds = None

    # If we have a token but it's missing required scopes, force a re-consent flow.
    # This is common when you initially authorize with gmail.readonly and later need
    # gmail.modify for label creation / message label changes.
    if creds and scopes:
        requested = set(scopes)

        # IMPORTANT: Credentials.from_authorized_user_file(..., scopes=...) can set
        # creds.scopes to the *requested* scopes even if the underlying refresh token
        # was granted fewer scopes. Prefer the scopes recorded in token.json.
        granted = set(token_file_scopes or (creds.scopes or []))

        # Gmail scopes are not strictly hierarchical strings, but for our usage we can
        # treat gmail.modify as a practical superset of gmail.readonly.
        if requested == {GMAIL_SCOPE_READONLY} and GMAIL_SCOPE_MODIFY in granted:
            requested = granted

        if not requested.issubset(granted):
            missing_scopes = requested - granted
            creds = None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except RefreshError:
            # Often caused by scope changes (e.g. requesting gmail.modify with a token
            # minted for gmail.readonly). Fall back to interactive auth if allowed.
            creds = None

    if not creds or not creds.valid:
        if not allow_interactive:
            if missing_scopes:
                granted_str = ", ".join(sorted(token_file_scopes or [])) or "<none>"
                missing_str = ", ".join(sorted(missing_scopes))
                raise RuntimeError(
                    "Gmail OAuth token is missing required scopes and interactive auth is disabled. "
                    f"Granted scopes in token.json: {granted_str}. Missing: {missing_str}. "
                    "Re-authorize with the required scopes (e.g. gmail.modify) to update token.json."
                )
            raise RuntimeError(
                "Gmail OAuth token is missing/invalid and interactive auth is disabled. "
                "Re-authorize to create/update token.json."
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

    def _html_to_text(html: str) -> str:
        """Best-effort HTML -> plain text.

        This is intentionally lightweight (stdlib-only) and designed for email bodies.
        """

        import html as _html
        import re

        s = html

        # Drop script/style blocks.
        s = re.sub(r"<\s*script[^>]*>.*?<\s*/\s*script\s*>", " ", s, flags=re.I | re.S)
        s = re.sub(r"<\s*style[^>]*>.*?<\s*/\s*style\s*>", " ", s, flags=re.I | re.S)

        # Common block breaks.
        s = re.sub(r"<\s*br\s*/?\s*>", "\n", s, flags=re.I)
        s = re.sub(r"</\s*p\s*>", "\n\n", s, flags=re.I)
        s = re.sub(r"</\s*div\s*>", "\n", s, flags=re.I)
        s = re.sub(r"</\s*li\s*>", "\n", s, flags=re.I)

        # Remove remaining tags.
        s = re.sub(r"<[^>]+>", " ", s)
        s = _html.unescape(s)

        # Collapse whitespace.
        s = re.sub(r"[\t\r\f\v]+", " ", s)
        s = re.sub(r"\n\s+", "\n", s)
        s = re.sub(r"\n{3,}", "\n\n", s)
        s = re.sub(r" {2,}", " ", s)
        return s.strip()

    def walk_parts(part: dict) -> tuple[list[str], list[str]]:
        mime = (part.get("mimeType") or "").lower()
        body = part.get("body", {}) or {}
        data = body.get("data")

        plain: list[str] = []
        html: list[str] = []

        if data and mime.startswith("text/plain"):
            plain.append(decode_b64(data))
        elif data and mime.startswith("text/html"):
            html.append(decode_b64(data))

        for p in part.get("parts", []) or []:
            p_plain, p_html = walk_parts(p)
            plain.extend(p_plain)
            html.extend(p_html)
        return plain, html

    payload = msg.get("payload", {}) or {}
    plain_texts, html_texts = walk_parts(payload)
    if plain_texts:
        combined = "\n\n".join(plain_texts).strip()
        return combined[:max_chars]

    if html_texts:
        combined_html = "\n\n".join(html_texts).strip()
        rendered = _html_to_text(combined_html)
        if rendered:
            return rendered[:max_chars]

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


def list_labels(service, *, user_id: str = "me") -> list[dict]:
    """Return all Gmail labels as raw dicts."""

    resp = service.users().labels().list(userId=user_id).execute()
    return list(resp.get("labels", []) or [])


def label_name_to_id(service, *, user_id: str = "me") -> dict[str, str]:
    """Return a mapping of Gmail label name -> label id."""

    labels = list_labels(service, user_id=user_id)
    out: dict[str, str] = {}
    for l in labels:
        lid = l.get("id")
        name = l.get("name")
        if lid and name:
            out[str(name)] = str(lid)
    return out


def create_label(service, *, name: str, user_id: str = "me") -> dict:
    """Create a Gmail label and return the created label resource."""

    body = {"name": name, "labelListVisibility": "labelShow", "messageListVisibility": "show"}
    return service.users().labels().create(userId=user_id, body=body).execute()


def update_label(service, *, label_id: str, name: str, user_id: str = "me") -> dict:
    """Update (rename) an existing Gmail label."""

    body = {"id": label_id, "name": name}
    return service.users().labels().update(userId=user_id, id=label_id, body=body).execute()


def modify_message_labels(
    service,
    *,
    message_id: str,
    add_label_ids: list[str] | None = None,
    remove_label_ids: list[str] | None = None,
    user_id: str = "me",
) -> dict:
    """Add/remove labels on a Gmail message.

    Note: Gmail "archive" semantics are implemented by removing the INBOX label.
    """

    body: dict[str, list[str]] = {}
    if add_label_ids:
        body["addLabelIds"] = list(add_label_ids)
    if remove_label_ids:
        body["removeLabelIds"] = list(remove_label_ids)
    return service.users().messages().modify(userId=user_id, id=message_id, body=body).execute()


def move_message_to_trash(
    service,
    *,
    message_id: str,
    user_id: str = "me",
) -> dict:
    """Move a Gmail message to Trash.

    In Gmail UI terms this is the same as clicking the Trash/Delete button.
    Messages in Trash are eligible for automatic permanent deletion after ~30 days.
    """

    return service.users().messages().trash(userId=user_id, id=message_id).execute()
