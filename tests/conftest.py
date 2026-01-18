"""Pytest configuration and shared fixtures."""

import pytest


@pytest.fixture
def mock_settings():
    """Provide mock settings for testing."""
    from email_manager_agent.config import Settings

    return Settings(
        ollama_host="http://test:11434",
        ollama_model="test-model",
        log_level="DEBUG",
        debug=True,
    )


@pytest.fixture
def sample_email_content() -> str:
    """Provide sample email content for testing."""
    return """
    Subject: Weekly Newsletter - Python Tips
    From: newsletter@python.org
    
    Welcome to this week's Python tips!
    
    In this issue:
    1. New features in Python 3.12
    2. Best practices for async programming
    3. Top Python libraries of 2024
    
    Unsubscribe: https://python.org/unsubscribe?id=123
    """


@pytest.fixture
def sample_email_data() -> dict:
    """Provide sample email data structure."""
    return {
        "id": "msg123456",
        "threadId": "thread789",
        "labelIds": ["INBOX", "UNREAD"],
        "snippet": "Weekly Newsletter - Python Tips",
        "payload": {
            "headers": [
                {"name": "Subject", "value": "Weekly Newsletter - Python Tips"},
                {"name": "From", "value": "newsletter@python.org"},
                {"name": "To", "value": "user@example.com"},
            ],
            "body": {"data": "encoded_body_data"},
        },
    }
