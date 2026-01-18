# Email Intelligence Dashboard

Metadata-first email analysis with a small FastAPI backend, a React dashboard, and a simple job runner.

High-level flow:

1. **Ingest metadata** from Gmail (no bodies) into Postgres + Qdrant.
2. **Cluster and label** unlabelled messages using embeddings + Ollama.
3. Explore results via a **hierarchy tree + sunburst** and inspect **sample message metadata**.

This is intentionally opinionated:

- Prefer *aggregation-first* and *metadata-first* workflows.
- Make uncertainty visible (e.g., “Pending labelling”).
- Avoid mailbox-modifying actions by default (uses `gmail.readonly`).

## What you get

Backend (FastAPI):

- `GET /health` and `GET /status`
- Dashboard data: `GET /api/dashboard/tree`
- Message samples: `GET /api/messages/samples?node_id=...`
- Jobs:
  - Start: `POST /api/jobs/ingest/full`, `POST /api/jobs/ingest/refresh`, `POST /api/jobs/cluster-label/run`
  - Status: `GET /api/jobs/{job_id}/status`, `GET /api/jobs/current`
  - Live progress (SSE): `GET /api/jobs/{job_id}/events`

Frontend (React/Vite):

- Dashboard page with hierarchy tree, sunburst, and details panel
- Jobs page (minimal placeholder; top bar shows active job + progress)

## Prerequisites

- Python 3.10+
- Docker + Docker Compose (Postgres + Qdrant)
- Node.js (for the UI)
- Gmail OAuth credentials + token files
- Ollama (recommended)
  - Labeling uses an LLM (`EMAIL_INTEL_OLLAMA_MODEL`)
  - Embeddings use a dedicated embedding model (default: `all-minilm`, 384 dims)

## Quickstart

### 1) Start Postgres + Qdrant

From `email-intelligence/docker/`:

```bash
docker compose up -d
```

### 2) Configure backend environment

Copy and edit:

```bash
cp backend/.env.example backend/.env
```

At minimum, set:

- `EMAIL_INTEL_GMAIL_CREDENTIALS_PATH`
- `EMAIL_INTEL_GMAIL_TOKEN_PATH`
- (optional) `EMAIL_INTEL_OLLAMA_HOST`, `EMAIL_INTEL_OLLAMA_MODEL`, `EMAIL_INTEL_EMBEDDING_MODEL`

### 3) Run the backend

From `email-intelligence/backend/`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8000
```

### 4) Run the frontend

From `email-intelligence/frontend/`:

```bash
npm install
npm run dev
```

The UI defaults to `http://localhost:5173` and expects the backend at `http://localhost:8000`.

## Notes

- **Ingestion is metadata-only.** Bodies are only fetched for representative samples during the labeling step.
- The job runner is **in-memory** (suitable for development and UI wiring).

## Explicit non-goals (for now)

- Unsubscribe actions
- Mailbox modification (labels/move/delete)
- Training/fine-tuning
