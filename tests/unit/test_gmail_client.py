"""Unit tests for Gmail client."""

import pytest

from email_manager_agent.gmail.client import GmailClient
from email_manager_agent.exceptions import AuthenticationError, ConfigurationError


class TestGmailClient:
    """Test suite for GmailClient class."""

    def test_gmail_client_initialization(self) -> None:
        """Test that Gmail client is properly initialized."""
        client = GmailClient()

        assert client.settings is not None
        assert client._service is None

    @pytest.mark.asyncio
    async def test_authenticate_missing_credentials_raises(self) -> None:
        """Test that authenticate fails fast when credentials.json is missing."""
        client = GmailClient()

        with pytest.raises(ConfigurationError):
            await client.authenticate()

    @pytest.mark.asyncio
    async def test_list_messages_requires_authentication(self) -> None:
        """Test that list_messages requires authenticate() first."""
        client = GmailClient()

        with pytest.raises(AuthenticationError):
            await client.list_messages()

    @pytest.mark.asyncio
    async def test_get_message_requires_authentication(self) -> None:
        """Test that get_message requires authenticate() first."""
        client = GmailClient()

        with pytest.raises(AuthenticationError):
            await client.get_message("msg123")
