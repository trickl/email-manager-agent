"""LLM prompt contract for Tier-1 labeling."""

from __future__ import annotations

from app.labeling.tier1 import TIER1_CATEGORIES


def build_label_prompt(
    *,
    sender_domain: str,
    subject_examples: list[str],
    cluster_size: int,
    frequency_label: str,
    unread_label: str,
    bodies: list[str],
) -> str:
    """Build the strict Tier-1 labeling prompt.

    The model MUST choose exactly one Tier-1 category (no Unknown).
    """

    subjects = "\n".join(f"- {s}" for s in subject_examples if s)
    bodies_block = "\n\n---\n\n".join(
        f"Body sample {i + 1}:\n{b.strip()}" for i, b in enumerate(bodies) if b.strip()
    )

    taxonomy = "\n".join(f"- {c}" for c in TIER1_CATEGORIES)

    return (
        "You are an email categorisation assistant.\n\n"
        "You are labelling a CLUSTER of emails based on representative samples and metadata.\n"
        "You must choose exactly ONE category from the fixed Tier-1 taxonomy below.\n"
        "There is NO 'Unknown' bucket. If none are perfect, choose the least wrong category.\n"
        "Optionally suggest a sub-category name (short).\n\n"
        f"Cluster size: {cluster_size}\n"
        f"Frequency label: {frequency_label}\n"
        f"Unread label: {unread_label}\n"
        f"Sender domain: {sender_domain}\n\n"
        "Normalized subject examples:\n"
        f"{subjects if subjects else '- (none)'}\n\n"
        "Representative email bodies:\n"
        f"{bodies_block if bodies_block else '(no bodies provided)'}\n\n"
        "Tier-1 taxonomy (choose ONE):\n"
        f"{taxonomy}\n\n"
        "Respond ONLY as JSON with this schema:\n"
        "{\n"
        '  "category": "<one of the Tier-1 categories>",\n'
        '  "subcategory": "<optional sub-category name or null>",\n'
        '  "confidence": <number between 0 and 1>\n'
        "}\n"
    )
