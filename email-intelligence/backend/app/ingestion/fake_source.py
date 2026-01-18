from datetime import datetime

from app.domain.email import EmailMessage
from app.utils.subject import normalize_subject


def fake_email() -> EmailMessage:
    subject = "Weekly Newsletter – January"

    return EmailMessage(
        gmail_message_id="fake-001",
        thread_id="thread-001",
        subject=subject,
        subject_normalized=normalize_subject(subject),
        from_address="news@example.com",
        from_domain="example.com",
        to_addresses=["me@personal.com"],
        cc_addresses=[],
        bcc_addresses=[],
        is_unread=True,
        internal_date=datetime.utcnow(),
        label_ids=["INBOX", "UNREAD"],
    )
