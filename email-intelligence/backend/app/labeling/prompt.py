"""LLM prompt contract for Tier-1 labeling."""

from __future__ import annotations

from app.labeling.tier1 import TIER1_CATEGORIES


def _render_tier2_taxonomy(tier2_options: dict[str, list[str]] | None) -> str:
    """Render Tier-1 and Tier-2 taxonomy for the prompt."""

    if not tier2_options:
        # Fallback: Tier-1 only.
        return "\n".join(f"- {c}" for c in TIER1_CATEGORIES)

    blocks: list[str] = []
    for cat in TIER1_CATEGORIES:
        subs = tier2_options.get(cat, [])
        if subs:
            blocks.append(f"- {cat}:")
            blocks.extend(f"  - {s}" for s in subs)
        else:
            blocks.append(f"- {cat}")
    return "\n".join(blocks)


def build_label_prompt(
    *,
    sender_domain: str,
    subject_examples: list[str],
    cluster_size: int,
    frequency_label: str,
    unread_label: str,
    bodies: list[str],
    tier2_options: dict[str, list[str]] | None = None,
) -> str:
    """Build the strict Tier-1 labeling prompt.

        The model MUST choose exactly one Tier-1 category (no Unknown).

        Response contract:
            - plain multiline text (NOT JSON)
            - exactly two non-empty lines
    """

    subjects = "\n".join(f"- {s}" for s in subject_examples if s)
    bodies_block = "\n\n---\n\n".join(
        f"Body sample {i + 1}:\n{b.strip()}" for i, b in enumerate(bodies) if b.strip()
    )

    taxonomy = _render_tier2_taxonomy(tier2_options)

    return (
        "You are an email categorisation assistant.\n\n"
        "You are labelling a CLUSTER of emails based on representative samples and metadata.\n"
        "You must choose exactly ONE category from the fixed Tier-1 taxonomy below.\n"
        f"IMPORTANT: Line 1 MUST be exactly one of: {', '.join(TIER1_CATEGORIES)}\n"
        "Do NOT put a Tier-2 label on line 1.\n"
        "There is NO 'Unknown' bucket. If none are perfect, choose the least wrong category.\n"
        "Choose a Tier-2 subcategory from the list under your chosen category whenever possible.\n"
        "If none fit, you MAY propose a new subcategory name (short) in the 'subcategory' field.\n"
        "Avoid inventing new subcategories unless necessary.\n\n"
        f"Cluster size: {cluster_size}\n"
        f"Frequency label: {frequency_label}\n"
        f"Unread label: {unread_label}\n"
        f"Sender domain: {sender_domain}\n\n"
        "Normalized subject examples:\n"
        f"{subjects if subjects else '- (none)'}\n\n"
        "Representative email bodies:\n"
        f"{bodies_block if bodies_block else '(no bodies provided)'}\n\n"
        "Tier-1 (and Tier-2) taxonomy:\n"
        f"{taxonomy}\n\n"
        "Respond ONLY as plain multiline text with EXACTLY TWO non-empty lines:\n"
        "Line 1: the chosen Tier-1 category (exactly as written in the taxonomy)\n"
        "Line 2: the Tier-2 subcategory (from the list under that category), or the word 'None'\n"
    )
