"""Backend configuration.

Phase 0 goal: keep configuration minimal and environment-driven.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from pydantic import BaseModel


load_dotenv()


class Settings(BaseModel):
    """Application settings loaded from environment variables."""

    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://email:email@localhost:5432/email_intelligence",
    )

    qdrant_host: str = os.getenv("QDRANT_HOST", "localhost")
    qdrant_port: int = int(os.getenv("QDRANT_PORT", "6333"))


def get_settings() -> Settings:
    return Settings()
