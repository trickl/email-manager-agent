"""Backend settings.

We keep configuration explicit and environment-driven so the pipeline is restartable and observable
across runs.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BACKEND_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    """Runtime configuration for the Email Intelligence backend."""

    model_config = SettingsConfigDict(
        env_prefix="EMAIL_INTEL_",
        # Always load the backend-local .env regardless of current working directory.
        env_file=str(BACKEND_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Core services
    database_url: str = "postgresql://email:email@localhost:5432/email_intelligence"
    qdrant_host: str = "localhost"
    qdrant_port: int = 6333

    # Gmail ingestion
    gmail_user_id: str = "me"
    gmail_credentials_path: str = "credentials.json"
    gmail_token_path: str = "token.json"
    gmail_page_size: int = 500

    # OAuth UX controls (useful for Gmail label sync which requires gmail.modify scope)
    gmail_allow_interactive: bool = True
    gmail_auth_mode: str = "local_server"  # local_server|console

    @field_validator("gmail_credentials_path", "gmail_token_path", mode="before")
    @classmethod
    def _resolve_local_paths(cls, v: str) -> str:
        p = Path(str(v))
        if p.is_absolute():
            return str(p)
        return str((BACKEND_ROOT / p).resolve())

    # Clustering/labeling
    similarity_threshold: float = 0.85
    label_version: str = "tier2-v1"

    # Development helpers
    seed_fake_email: bool = False

    # Optional local LLM (Ollama)
    ollama_host: str | None = None
    ollama_model: str = "llama3.1:8b"

    # Embeddings
    # Default to a small embedding model that matches our historical VECTOR_SIZE=384.
    # Ensure it's available locally: `ollama pull all-minilm`.
    embedding_model: str = "all-minilm"
    embedding_timeout_seconds: int = 60

    # Safety valve: allow falling back to deterministic dummy vectors when Ollama embeddings
    # are unavailable. Prefer leaving this False in normal operation.
    allow_deterministic_vectors: bool = False


class StatusResponse(BaseModel):
    current_phase: str | None
    total_email_count: int
    labelled_email_count: int
    unlabelled_email_count: int
    cluster_count: int
    estimated_remaining_clusters: int
    last_ingested_internal_date: str | None
