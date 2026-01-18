"""Unit tests for configuration module."""

import pytest

from email_manager_agent.config import Settings, get_settings


class TestSettings:
    """Test suite for Settings class."""

    def test_default_settings(self) -> None:
        """Test that default settings are properly initialized."""
        settings = Settings()

        assert settings.ollama_host == "http://localhost:11434"
        assert settings.ollama_model == "llama2"
        assert settings.log_level == "INFO"
        assert settings.debug is False
        assert settings.max_retries == 3

    def test_settings_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading settings from environment variables."""
        monkeypatch.setenv("EMAIL_AGENT_OLLAMA_HOST", "http://custom:8080")
        monkeypatch.setenv("EMAIL_AGENT_LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("EMAIL_AGENT_DEBUG", "true")

        # Clear the cache to ensure fresh settings
        get_settings.cache_clear()

        settings = get_settings()

        assert settings.ollama_host == "http://custom:8080"
        assert settings.log_level == "DEBUG"
        assert settings.debug is True

        # Clean up
        get_settings.cache_clear()

    def test_get_settings_returns_cached_instance(self) -> None:
        """Test that get_settings returns the same cached instance."""
        get_settings.cache_clear()

        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2

        # Clean up
        get_settings.cache_clear()
