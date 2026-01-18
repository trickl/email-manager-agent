"""Unit tests for Gmail metadata parsing helpers."""

from email_manager_agent.gmail.parsing import message_to_email_header


def test_message_to_email_header_parses_basic_fields(sample_email_data) -> None:
    header = message_to_email_header(sample_email_data)

    assert header.gmail_id == "msg123456"
    assert header.thread_id == "thread789"
    assert header.subject == "Weekly Newsletter - Python Tips"
    assert header.from_email == "newsletter@python.org"
    assert header.to_addrs == ["user@example.com"]
    assert header.is_unread is True
    assert header.is_inbox is True
