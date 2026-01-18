"""Unit tests for data models."""

from datetime import datetime

import pytest

from email_manager_agent.models import (
    EmailCategorization,
    EmailCategory,
    EmailMessage,
    ImportanceLevel,
    ProcessingResult,
)


class TestEmailMessage:
    """Test suite for EmailMessage model."""

    def test_email_message_creation(self) -> None:
        """Test creating an EmailMessage instance."""
        email = EmailMessage(
            id="msg123",
            thread_id="thread456",
            subject="Test Email",
            sender="sender@example.com",
            recipient="recipient@example.com",
            date=datetime.now(),
            body="This is a test email.",
            labels=["INBOX", "UNREAD"],
        )

        assert email.id == "msg123"
        assert email.subject == "Test Email"
        assert len(email.labels) == 2


class TestEmailCategorization:
    """Test suite for EmailCategorization model."""

    def test_email_categorization_creation(self) -> None:
        """Test creating an EmailCategorization instance."""
        categorization = EmailCategorization(
            category=EmailCategory.NEWSLETTER,
            importance=ImportanceLevel.LOW,
            confidence=0.85,
            reasoning="Contains newsletter indicators",
            suggested_actions=["Archive", "Unsubscribe"],
            is_subscription=True,
            unsubscribe_link="https://example.com/unsubscribe",
        )

        assert categorization.category == EmailCategory.NEWSLETTER
        assert categorization.importance == ImportanceLevel.LOW
        assert categorization.confidence == 0.85
        assert categorization.is_subscription is True

    def test_confidence_validation(self) -> None:
        """Test that confidence score is properly validated."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            EmailCategorization(
                category=EmailCategory.SPAM,
                importance=ImportanceLevel.LOW,
                confidence=1.5,  # Invalid: > 1.0
                reasoning="Test",
            )


class TestProcessingResult:
    """Test suite for ProcessingResult model."""

    def test_processing_result_creation(self) -> None:
        """Test creating a ProcessingResult instance."""
        categorization = EmailCategorization(
            category=EmailCategory.PERSONAL,
            importance=ImportanceLevel.HIGH,
            confidence=0.95,
            reasoning="Personal email from known contact",
        )

        result = ProcessingResult(
            email_id="msg789",
            categorization=categorization,
            processing_time_ms=150.5,
            success=True,
        )

        assert result.email_id == "msg789"
        assert result.success is True
        assert result.processing_time_ms == 150.5
        assert result.categorization.category == EmailCategory.PERSONAL
