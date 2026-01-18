"""Vectorization / embeddings.

Historically this project used deterministic pseudo-random vectors derived from the embedding text
contract (hash-seeded RNG). That was useful for restartability but NOT semantically meaningful.

We now prefer real embeddings via Ollama's embeddings API, while keeping the deterministic
implementation as an explicit opt-in fallback for development.
"""

from __future__ import annotations

import hashlib
import math
import random

import json
import urllib.error
import urllib.request
from functools import lru_cache


from app.settings import Settings

VECTOR_SIZE = 384



@lru_cache
def _settings() -> Settings:
    return Settings()


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0.0:
        return vec
    return [v / norm for v in vec]


def vectorize_text_deterministic(text: str, size: int = VECTOR_SIZE) -> list[float]:
    """Generate a deterministic unit-length vector from text.

    This is a non-semantic fallback. Only use when explicitly enabled via settings.

    Args:
        text: Input text (must follow the locked embedding text contract).
        size: Vector dimensionality.

    Returns:
        Unit-length vector of floats.
    """

    digest = hashlib.sha256(text.encode("utf-8")).digest()
    seed = int.from_bytes(digest[:8], "big", signed=False)

    rng = random.Random(seed)
    vec = [rng.random() for _ in range(size)]
    return _normalize(vec)


def _ollama_embeddings(host: str, *, model: str, text: str, timeout: int) -> list[float]:
    """Call Ollama embeddings API and return a single embedding vector."""

    host = host.rstrip("/")

    # Older Ollama API: /api/embeddings with {model, prompt}
    payload = json.dumps({"model": model, "prompt": text}).encode("utf-8")
    req = urllib.request.Request(
        url=f"{host}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8"))
        emb = data.get("embedding")
        if not isinstance(emb, list) or not emb:
            raise ValueError("Ollama embeddings response missing 'embedding'")
        return [float(x) for x in emb]
    except urllib.error.HTTPError as e:
        # Newer Ollama API may use /api/embed with {model, input}
        if e.code != 404:
            raise

    payload2 = json.dumps({"model": model, "input": text}).encode("utf-8")
    req2 = urllib.request.Request(
        url=f"{host}/api/embed",
        data=payload2,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req2, timeout=timeout) as resp:  # noqa: S310
        data = json.loads(resp.read().decode("utf-8"))
    # /api/embed may return {embeddings: [[...]]}
    embs = data.get("embeddings")
    if isinstance(embs, list) and embs and isinstance(embs[0], list):
        emb = embs[0]
        return [float(x) for x in emb]

    raise ValueError("Ollama embed response missing 'embeddings'")


def vectorize_text(text: str, size: int = VECTOR_SIZE) -> list[float]:
    """Vectorize text into a unit-length embedding.

    Preferred path: Ollama embeddings.
    Fallback (opt-in): deterministic pseudo-random vectors.
    """

    s = _settings()
    if s.ollama_host:
        vec = _ollama_embeddings(
            s.ollama_host,
            model=s.embedding_model,
            text=text,
            timeout=int(s.embedding_timeout_seconds),
        )
        if len(vec) != size:
            raise ValueError(
                f"Embedding size mismatch: got {len(vec)}, expected {size}. "
                f"Either use an embedding model with {size} dims (default: all-minilm) "
                f"or migrate the Qdrant collection to the new dimension."
            )
        return _normalize(vec)

    if s.allow_deterministic_vectors:
        return vectorize_text_deterministic(text, size=size)

    raise RuntimeError(
        "Embeddings are not configured. Set EMAIL_INTEL_OLLAMA_HOST and ensure an embedding model "
        "is available (e.g. `ollama pull all-minilm`). To allow non-semantic fallback vectors, set "
        "EMAIL_INTEL_ALLOW_DETERMINISTIC_VECTORS=true."
    )


def vector_version_tag() -> str:
    """Return a tag describing what produced vectors in this process.

    This is stored in Qdrant payloads so we can filter similarity searches to only use
    semantically meaningful vectors once backfilled.
    """

    s = _settings()
    if s.ollama_host:
        return f"ollama:{s.embedding_model}"
    return "deterministic"
