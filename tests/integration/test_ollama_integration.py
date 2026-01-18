"""Placeholder for integration tests with Ollama.

Integration tests will be added once the Ollama client is implemented.
These tests require a running Ollama instance.
"""

import pytest


@pytest.mark.integration
class TestOllamaIntegration:
    """Integration tests for Ollama LLM."""

    @pytest.mark.skip(reason="Ollama client not yet implemented")
    def test_ollama_connection(self) -> None:
        """Test connection to Ollama instance."""
        pass

    @pytest.mark.skip(reason="Ollama client not yet implemented")
    def test_email_categorization_with_ollama(self) -> None:
        """Test email categorization using real Ollama inference."""
        pass
