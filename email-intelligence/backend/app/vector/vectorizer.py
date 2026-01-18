"""Deterministic vectorization.

For now, embeddings are generated deterministically from the locked embedding text contract.
This preserves restartability and makes clustering repeatable "where possible" without requiring
an external embedding model.

When real embeddings are introduced later, keep `build_embedding_text()` stable and swap the
implementation here.
"""

from __future__ import annotations

import hashlib
import math
import random


VECTOR_SIZE = 384


def vectorize_text(text: str, size: int = VECTOR_SIZE) -> list[float]:
    """Generate a deterministic unit-length vector from text.

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

    # Normalize to unit length for cosine distance.
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0.0:
        return vec
    return [v / norm for v in vec]
