"""Configuration management for Email Manager Agent.

This module handles application configuration using Pydantic settings.
Configuration can be loaded from environment variables or .env files.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support.

    All settings can be overridden via environment variables with
    the EMAIL_AGENT_ prefix (e.g., EMAIL_AGENT_OLLAMA_HOST).
    """

    model_config = SettingsConfigDict(
        env_prefix="EMAIL_AGENT_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Ollama Configuration
    ollama_host: str = Field(
        default="http://localhost:11434",
        description="Ollama API host URL",
    )
    ollama_model: str = Field(
        default="llama2",
        description="Default Ollama model to use for inference",
    )
    ollama_timeout: int = Field(
        default=30,
        description="Timeout for Ollama API requests in seconds",
    )

    # Gmail Configuration
    gmail_credentials_path: Path = Field(
        default=Path("credentials.json"),
        description="Path to Gmail API credentials file",
    )
    gmail_token_path: Path = Field(
        default=Path("token.json"),
        description="Path to Gmail API token file",
    )
    gmail_max_results: int = Field(
        default=100,
        description="Maximum number of emails to fetch per request",
    )

    # Application Configuration
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for failed operations",
    )
    cache_enabled: bool = Field(
        default=True,
        description="Enable response caching",
    )
    cache_ttl: int = Field(
        default=3600,
        description="Cache time-to-live in seconds",
    )

    # Database Configuration (for future use)
    database_url: str | None = Field(
        default=None,
        description="Database URL for storing categorization history",
    )


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings.

    Returns:
        Settings: Application settings instance.
    """
    return Settings()
