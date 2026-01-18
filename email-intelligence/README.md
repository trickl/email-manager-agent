# Email Intelligence System – Phase 0/1 (Live Wiring)

Objective

Stand up a fully running system with:

- A Python backend (virtualenv-based)
- Gmail API client wired (no ingestion yet)
- PostgreSQL running, reachable, with one real table
- Qdrant running, reachable, with one empty collection
- React + Vite UI rendering a live but empty dashboard
- Docker used only for services (Postgres, Qdrant)

No fake services. No mocks. No production logic.

Phase 1 adds a stable Python domain model and a single fake-email ingestion flow
to prove Postgres + Qdrant wiring end-to-end.

## Repository layout (mandatory)

This directory contains the required layout:

```
email-intelligence/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── config.py
│   │   ├── db/
│   │   │   ├── __init__.py
│   │   │   └── postgres.py
│   │   ├── vector/
│   │   │   ├── __init__.py
│   │   │   └── qdrant.py
│   │   └── gmail/
│   │       ├── __init__.py
│   │       └── client.py
│   ├── requirements.txt
│   └── .env.example
│
├── frontend/
│   ├── index.html
│   ├── package.json
│   ├── vite.config.ts
│   └── src/
│       ├── main.tsx
│       ├── App.tsx
│       └── components/
│           └── EmptyDashboard.tsx
│
├── docker/
│   ├── docker-compose.yml
│   └── postgres/
│       └── init.sql
│
└── README.md
```

## Step 1: Python virtual environment (backend)

Commands:

- `cd backend`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install --upgrade pip`

Install dependencies:

- `pip install -r requirements.txt`

## Step 2: Backend stub (FastAPI)

The backend is a minimal FastAPI app with a health endpoint.

Run command:

- `uvicorn app.main:app --reload`

Health endpoint:

- `GET http://localhost:8000/health` returns `{ "status": "ok" }`

## Step 3: PostgreSQL (real)

Start services:

- `cd ../docker`
- `docker compose up`

Note: `docker compose` must be run in the directory that contains `docker-compose.yml`
(i.e. `email-intelligence/docker/`), or you must provide an explicit `-f` path.

This brings up Postgres and creates a real schema via `docker/postgres/init.sql`:

- Table: `email_message` (authoritative Phase 1 schema)

Note: the backend imports `app.db.postgres` which tests the DB connection on import.
If Postgres is not running, the backend will fail fast.

## Step 4: Qdrant (real, empty)

Qdrant is started via Docker Compose and is reachable on:

- `http://localhost:6333`

On backend startup, `app.vector.qdrant.ensure_collection()` runs once to ensure an empty
collection exists:

- Collection: `email_subjects` (vector size 384, cosine distance)

## Step 5: Gmail API client (metadata ingestion)

`backend/app/gmail/client.py` implements:

- Phase 1 metadata ingestion (no bodies): `users.messages.list` + `users.messages.get(format=metadata)`
- Phase 2 representative sampling: body fetch is performed only for cluster analysis/labeling

You will need local OAuth credentials + token files to run ingestion.

## Step 6: Frontend (React + Vite)

Bootstrap commands:

- `cd frontend`
- `npm install`
- `npm run dev`

The UI renders a live but empty dashboard with:

- “No email data indexed yet.”
- “Connect Gmail to begin analysis.”

## Step 7: Environment configuration

Copy `backend/.env.example` to `backend/.env` (or export variables) and adjust if needed.

Preferred variables are namespaced:

- `EMAIL_INTEL_DATABASE_URL=postgresql://email:email@localhost:5432/email_intelligence`
- `EMAIL_INTEL_QDRANT_HOST=localhost`
- `EMAIL_INTEL_QDRANT_PORT=6333`
- `EMAIL_INTEL_GMAIL_CREDENTIALS_PATH=credentials.json`
- `EMAIL_INTEL_GMAIL_TOKEN_PATH=token.json`

Optional development helper:

- `EMAIL_INTEL_SEED_FAKE_EMAIL=true` (runs the original fake seed on startup)

## Step 8: Definition of “done” (strict)

- `docker compose up` starts Postgres + Qdrant
- Postgres contains table `email_message` (canonical store)
- Postgres contains table `taxonomy_label` (Tier-1 taxonomy seed)
- Postgres contains table `pipeline_kv` (checkpoint/phase)
- Postgres contains table `email_cluster` (cluster identity)
- Qdrant collection `email_subjects` exists
- FastAPI `/health` returns `{ "status": "ok" }`
- FastAPI `/status` returns counts and checkpoint information

To run the pipeline explicitly:

- `POST /pipeline/ingest-metadata` ingests metadata-only into Postgres + Qdrant
- `POST /pipeline/cluster-label` clusters and labels emails (bodies are fetched only for samples)
- React UI loads and shows empty dashboard

## Explicit non-goals

- No UI clustering logic
- No unsubscribe actions
- No model fine-tuning
- No heuristic spam rules
- No background jobs
- No unsubscribe logic
- No auth UI
- No dashboards with fake numbers
