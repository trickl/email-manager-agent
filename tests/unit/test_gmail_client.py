"""Unit tests for Gmail client."""

import pytest

from email_manager_agent.gmail.client import GmailClient


class TestGmailClient:
    """Test suite for GmailClient class."""

    def test_gmail_client_initialization(self) -> None:
        """Test that Gmail client is properly initialized."""
        client = GmailClient()

        assert client.settings is not None
        assert client._service is None

    @pytest.mark.asyncio
    async def test_authenticate_not_implemented(self) -> None:
        """Test that authenticate raises NotImplementedError."""
        client = GmailClient()

        with pytest.raises(NotImplementedError):
            await client.authenticate()

    @pytest.mark.asyncio
    async def test_list_messages_not_implemented(self) -> None:
        """Test that list_messages raises NotImplementedError."""
        client = GmailClient()

        with pytest.raises(NotImplementedError):
            await client.list_messages()

    @pytest.mark.asyncio
    async def test_get_message_not_implemented(self) -> None:
        """Test that get_message raises NotImplementedError."""
        client = GmailClient()

        with pytest.raises(NotImplementedError):
            await client.get_message("msg123")
