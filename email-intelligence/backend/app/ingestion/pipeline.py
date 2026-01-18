from app.ingestion.fake_source import fake_email
from app.repository.email_repository import insert_email
from app.vector.embedding import build_embedding_text
from app.vector.qdrant import upsert_email
from app.vector.vectorizer import vectorize_text


def ingest_fake_email():
    email = fake_email()

    # 1. Persist metadata
    insert_email(email)

    # 2. Build stable embedding input text (contract)
    embedding_text = build_embedding_text(email)

    # 3. Generate deterministic vector
    vector = vectorize_text(embedding_text)

    # 4. Upsert into vector DB
    upsert_email(email, vector)
