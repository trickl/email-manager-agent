"""(Removed) Trash/lifecycle API.

The system no longer manages generic lifecycle transitions (trash/expire/untrash).
The only hygiene action in scope is Gmail archiving driven by taxonomy retention.

This module is intentionally dependency-free and is NOT wired into the FastAPI app.
"""

REMOVED = True

