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
            internal_date,
            label_ids
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
            :internal_date,
            :label_ids
        )
        ON CONFLICT (gmail_message_id) DO UPDATE
        SET
            thread_id = EXCLUDED.thread_id,
            subject = EXCLUDED.subject,
            subject_normalized = EXCLUDED.subject_normalized,
            from_address = EXCLUDED.from_address,
            from_domain = EXCLUDED.from_domain,
            to_addresses = EXCLUDED.to_addresses,
            cc_addresses = EXCLUDED.cc_addresses,
            bcc_addresses = EXCLUDED.bcc_addresses,
            is_unread = EXCLUDED.is_unread,
            internal_date = EXCLUDED.internal_date,
            label_ids = EXCLUDED.label_ids
        """
    )

    with engine.begin() as conn:
        conn.execute(query, vars(email))
