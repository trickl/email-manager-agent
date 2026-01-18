from sqlalchemy import text

from app.db.postgres import engine
from app.domain.email import EmailMessage


def insert_email(email: EmailMessage) -> None:
    query = text(
        """
        INSERT INTO email_message (
            gmail_message_id,
            thread_id,
            subject,
            subject_normalized,
            from_address,
            from_domain,
            to_addresses,
            cc_addresses,
            bcc_addresses,
            is_unread,
            internal_date
        )
        VALUES (
            :gmail_message_id,
            :thread_id,
            :subject,
            :subject_normalized,
            :from_address,
            :from_domain,
            :to_addresses,
            :cc_addresses,
            :bcc_addresses,
            :is_unread,
            :internal_date
        )
        ON CONFLICT (gmail_message_id) DO NOTHING
        """
    )

    with engine.begin() as conn:
        conn.execute(query, vars(email))
