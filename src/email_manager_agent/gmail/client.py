"""Gmail API client implementation.

This module provides a client for interacting with the Gmail API.

Notes:
    The Google API client is synchronous. This project wraps those calls using
    `asyncio.to_thread` so the rest of the codebase can remain async-friendly.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import structlog

from email_manager_agent.config import Settings
from email_manager_agent.exceptions import AuthenticationError, ConfigurationError, GmailAPIError

logger = structlog.get_logger()


class GmailClient:
    """Gmail API client for email operations.

    This client handles authentication, message retrieval,
    and other Gmail operations.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """Initialize Gmail client.

        Args:
            settings: Application settings. If None, uses default settings.
        """
        from email_manager_agent.config import get_settings

        self.settings = settings or get_settings()
        self._service: Any | None = None
        logger.info("gmail_client_initialized")

    async def authenticate(self) -> None:
        """Authenticate with Gmail API using OAuth2.

        Raises:
            AuthenticationError: If authentication fails.
        """

        if self._service is not None:
            return

        credentials_path = Path(self.settings.gmail_credentials_path)
        token_path = Path(self.settings.gmail_token_path)
        scope = self.settings.gmail_scope

        if not credentials_path.exists():
            raise ConfigurationError(
                f"Gmail credentials file not found: {credentials_path}. "
                "See README.md -> Gmail API Setup."
            )

        logger.info(
            "gmail_authentication_started",
            credentials_path=str(credentials_path),
            token_path=str(token_path),
            scope=scope,
        )

        try:
            self._service = await asyncio.to_thread(
                self._build_service,
                credentials_path,
                token_path,
                scope,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("gmail_authentication_failed", error=str(exc))
            raise AuthenticationError(str(exc)) from exc

        logger.info("gmail_authentication_completed")

    async def list_messages(
        self,
        max_results: int | None = None,
        query: str | None = None,
    ) -> list[dict[str, Any]]:
        """List messages from Gmail.

        Args:
            max_results: Maximum number of messages to return.
            query: Gmail search query string.

        Returns:
            List of message metadata dictionaries.

        Raises:
            GmailAPIError: If the API request fails.
        """

        await self._ensure_authenticated()

        resolved_max = max_results
        if resolved_max is None:
            logger.info("listing_messages", max_results="all", query=query)
        else:
            logger.info("listing_messages", max_results=resolved_max, query=query)

        try:
            return await asyncio.to_thread(self._list_messages_sync, resolved_max, query)
        except Exception as exc:  # noqa: BLE001
            logger.exception("gmail_list_messages_failed", error=str(exc))
            raise GmailAPIError(str(exc)) from exc

    async def get_message(
        self,
        message_id: str,
        *,
        format: str = "metadata",
        metadata_headers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get a specific message by ID.

        Args:
            message_id: The Gmail message ID.

        Returns:
            Message data dictionary.

        Raises:
            GmailAPIError: If the API request fails.
        """

        await self._ensure_authenticated()

        logger.info("getting_message", message_id=message_id, format=format)

        try:
            return await asyncio.to_thread(
                self._get_message_sync,
                message_id,
                format,
                metadata_headers,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("gmail_get_message_failed", message_id=message_id, error=str(exc))
            raise GmailAPIError(str(exc)) from exc

    async def _ensure_authenticated(self) -> None:
        if self._service is None:
            raise AuthenticationError(
                "Gmail client is not authenticated. Call await GmailClient.authenticate() first."
            )

    def _build_service(self, credentials_path: Path, token_path: Path, scope: str) -> Any:
        # Imported lazily to keep import-time cost low and tests fast.
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build

        creds: Credentials | None = None
        if token_path.exists():
            creds = Credentials.from_authorized_user_file(str(token_path), scopes=[scope])

        if creds is not None and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        if creds is None or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), scopes=[scope])
            creds = flow.run_local_server(port=0)
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(creds.to_json(), encoding="utf-8")

        # cache_discovery=False prevents writing discovery docs to disk.
        return build("gmail", "v1", credentials=creds, cache_discovery=False)

    def _list_messages_sync(self, max_results: int | None, query: str | None) -> list[dict[str, Any]]:
        assert self._service is not None
        user_id = "me"
        messages: list[dict[str, Any]] = []

        page_token: str | None = None
        while True:
            if max_results is not None and len(messages) >= max_results:
                break

            remaining = None if max_results is None else max_results - len(messages)
            per_page = 500 if remaining is None else min(500, remaining)

            request = (
                self._service.users()
                .messages()
                .list(userId=user_id, maxResults=per_page, q=query, pageToken=page_token)
            )
            response = request.execute()
            messages.extend(response.get("messages", []) or [])
            page_token = response.get("nextPageToken")
            if page_token is None:
                break

        return messages if max_results is None else messages[:max_results]

    def _get_message_sync(
        self,
        message_id: str,
        format: str,
        metadata_headers: list[str] | None,
    ) -> dict[str, Any]:
        assert self._service is not None
        user_id = "me"
        request = (
            self._service.users()
            .messages()
            .get(userId=user_id, id=message_id, format=format, metadataHeaders=metadata_headers)
        )
        return request.execute()
