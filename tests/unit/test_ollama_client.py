"""Unit tests for Ollama client."""

import pytest

from email_manager_agent.ollama.client import OllamaClient


class TestOllamaClient:
    """Test suite for OllamaClient class."""

    def test_ollama_client_initialization(self) -> None:
        """Test that Ollama client is properly initialized."""
        client = OllamaClient()

        assert client.settings is not None
        assert client.settings.ollama_host == "http://localhost:11434"

    @pytest.mark.asyncio
    async def test_generate_not_implemented(self) -> None:
        """Test that generate raises NotImplementedError."""
        client = OllamaClient()

        with pytest.raises(NotImplementedError):
            await client.generate("Test prompt")

    @pytest.mark.asyncio
    async def test_chat_not_implemented(self) -> None:
        """Test that chat raises NotImplementedError."""
        client = OllamaClient()

        messages = [{"role": "user", "content": "Hello"}]
        with pytest.raises(NotImplementedError):
            await client.chat(messages)
