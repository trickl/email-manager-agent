def build_embedding_text(email) -> str:
    subject = email.subject_normalized or ""
    return f"Subject: {subject}.\nSender domain: {email.from_domain}.\n"
