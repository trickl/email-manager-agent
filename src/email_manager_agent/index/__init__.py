"""Header-only email indexing.

This package contains functionality to build and query a local index of email
metadata (subjects, senders, recipients, labels, read/unread, etc.) without
storing full message bodies.
"""

from .repository import EmailIndexRepository

__all__ = ["EmailIndexRepository"]
