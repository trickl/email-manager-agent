"""Unit tests for email agent."""

import pytest

from email_manager_agent.agent.email_agent import EmailAgent


class TestEmailAgent:
    """Test suite for EmailAgent class."""

    def test_email_agent_initialization(self) -> None:
        """Test that email agent is properly initialized."""
        agent = EmailAgent()

        assert agent.settings is not None
        assert agent.gmail_client is not None
        assert agent.ollama_client is not None

    @pytest.mark.asyncio
    async def test_process_emails_not_implemented(self) -> None:
        """Test that process_emails raises NotImplementedError."""
        agent = EmailAgent()

        with pytest.raises(NotImplementedError):
            await agent.process_emails()

    @pytest.mark.asyncio
    async def test_categorize_email_not_implemented(self) -> None:
        """Test that categorize_email raises NotImplementedError."""
        agent = EmailAgent()

        with pytest.raises(NotImplementedError):
            await agent.categorize_email("Test email content")
