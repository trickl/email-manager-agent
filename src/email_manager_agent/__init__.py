"""Email Manager Agent - AI-powered email management automation.

This package provides tools for automatically categorizing, managing,
and cleaning up email inboxes using Ollama LLM and Gmail API.
"""

__version__ = "0.1.0"
__author__ = "Trickl"

from email_manager_agent.config import Settings, get_settings

__all__ = ["Settings", "get_settings", "__version__", "__author__"]
