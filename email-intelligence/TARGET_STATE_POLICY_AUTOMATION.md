# Policy-Driven Email Automation — Target State (North Star)

**Status:** North-star / target state (not an implementation guide)

This document defines the intended end-state for a **policy-driven email automation engine**.
It is not an email client replacement. It treats email as data and performs safe, auditable,
reversible actions at scale.

> Implementation note: we will begin by building **Stage 1** and **Stage 2** first, backed by
> **PostgreSQL**, to safely clean up a large backlog of historical email before moving on to
> more sophisticated automation for new inbound messages.

## 1) Consolidated Summary (What we’re building)

We are building a system that:

- Operates on **email metadata** (headers and derived fields), not message-body reading.
- Is **non-destructive by default**:
  - “Delete” means *move to a recoverable state*.
  - Permanent deletion only occurs after a **retention window**.
- Is **policy-driven** rather than hard-coded:
  - Users define **rules (conditions)** and **actions**.
  - Policies can run **on ingestion (real time)** and/or **on a schedule (batch)**.
- Provides **minimal UI**, optimized for bulk operations and overview:
  - No rendered email bodies.
  - Bulk actions over grids and summaries.
- Evolves progressively:
  - Start with deterministic rules.
  - Later introduce probabilistic / ML-assisted suggestions.
  - Advanced actions (unsubscribe, calendar, financial insights) are layered on top.

## 2) Conceptual Model (Implicit Architecture)

### 2.1 Email Lifecycle States

We model messages with explicit lifecycle states, independent of provider folders/labels.
Provider state is *observed*; system state is tracked in our datastore.

**States**

- **ACTIVE**
  - Message is considered live/retained.

- **TRASHED** (soft-delete)
  - Message is moved to a recoverable state.
  - Must record:
    - `trashed_at`
    - `trashed_by_policy_id` (or equivalent provenance)
    - `expiry_at` (computed from retention)

- **EXPIRED** (hard-delete eligible / deleted)
  - Occurs after retention expiry (e.g. 30 days in trash).
  - Requires explicit policy ownership.

**Implications**

- We require a **shadow metadata store** (Postgres) tracking state transitions.
- Email provider folders alone are not sufficient as the source of truth.
- Every state transition should be **auditable** and **reversible** (within retention).

### 2.2 Policy Engine Capabilities

A policy consists of:

**Conditions (deterministic predicates)**

- Category / classification (e.g. marketing)
- Age (e.g. > 6 months)
- Sender / domain
- Subject contains / excludes
- Presence of unsubscribe link (or header)
- Read / unread / interacted-with

**Logical composition**

- AND / OR
- UNLESS (exception clauses)

**Triggers**

- On receipt / ingestion (real-time)
- Scheduled (daily, weekly, custom cadence)

**Actions**

- Move to trash (soft delete)
- Un-trash (manual only)
- Unsubscribe (multi-step)
- Create calendar event
- Extract structured data (tickets/receipts/statements)

### 2.3 Action Complexity Gradient

Policies can invoke actions across a spectrum of complexity:

1) **Simple (local, deterministic)**
   - Move to folder/label
   - Tag/label in DB
   - Mark state in DB

2) **Transactional (provider-scoped APIs)**
   - Unsubscribe via RFC headers or provider-supported flows
   - Calendar creation via API

3) **Agentic / browser-like**
   - Follow unsubscribe links
   - Click confirmation buttons
   - Handle retries, confirmations, interstitials

Agentic actions require stronger guardrails, observability, and replayability.

## 3) Staged Action Plan (Value-first, risk-managed)

### Stage 1 — Foundation: Safety, State, Observability

**Goal:** Make deletion safe, reversible, and auditable.

**Build**

- Email metadata ingestion (headers only).
- Internal state model: `ACTIVE`, `TRASHED`, `EXPIRED`.
- Trash semantics tracked in Postgres:
  - `trashed_at`, `expiry_at`, `trashed_by_policy_id`.
- Retention daemon:
  - Periodically hard-delete items whose retention has expired.
- Minimal UI:
  - “Trash view” grid: sender, subject, date, `trashed_at`, age.
  - Bulk **undelete** only.

**Value**

- Immediate cleanup without fear.
- Establishes trust and safety for all future automation.

### Stage 2 — Deterministic Policy Engine (Rules v1)

**Goal:** Automate obvious wins without ML or agents.

**Build**

- Policy definition schema:
  - Conditions: age, category, sender, subject
  - Actions: move to trash
  - Trigger type: on receipt / scheduled
- Policy scheduler:
  - Start with weekly batch evaluation over historical email.
- Canonical policy example:
  - If category = marketing AND age > 6 months → move to trash

**Value**

- Prevents inbox decay.
- Clear mental model; low risk.

### Stage 3 — Real-time + Exceptions

**Goal:** Policies become expressive enough to trust.

**Build**

- Logical operators: AND, OR, UNLESS
- Exception clauses:
  - “Unless from X”
  - “Unless subject contains Y”
- On-receipt evaluation pipeline.

**Value**

- “Set and forget” policies.
- Fewer false positives; better alignment with human intent.

### Stage 4 — Unsubscribe Intelligence (Action v2)

**Goal:** Reduce future noise, not just clean history.

**Build**

- Unsubscribe detection:
  - RFC headers
  - Body heuristics (only when required)
- Safe unsubscribe action:
  - Prefer provider-level unsubscribe
  - Fall back to link-following
- Guardrails:
  - Rate limiting
  - Dry-run/preview mode
  - Sensitive-sender blacklist

**Value**

- Compounding inbox quality improvement.

### Stage 5 — Agentic Actions (Controlled Automation)

**Goal:** Enable actions that require external interaction.

**Build**

- Action runner abstraction:
  - Retry logic
  - Timeouts
  - Idempotency
- Browser-like unsubscribe agent.
- Per-action audit log:
  - Inputs
  - Outcome
  - Confidence

**Value**

- Handles the long tail of awkward unsubscribe flows.

### Stage 6 — Event Extraction & Calendar Integration

**Goal:** Turn emails into time-aware structure.

**Build**

- Event detection (tickets/reservations/bookings)
- Structured extraction (date/time/location)
- Calendar write-back
- UI: “Events created by email” view

**Value**

- Direct productivity gain; low risk.

### Stage 7 — Financial Analysis Layer

**Goal:** Treat email as a passive financial ledger.

**Build**

- Statement detection and classification
- Incremental aggregation:
  - Vendor
  - Frequency
  - Amount
- Financial dashboard:
  - Spend over time
  - Subscription detection

**Value**

- Turns inbox history into insight.

## 4) Guiding Design Constraints (Must-haves)

- Every destructive action must be reversible for **≥ 30 days**.
- No policy runs without an **owning policy ID**.
- No agentic action without **logging + replayability**.
- UI is optimized for **bulk/overview**, not reading email content.
- ML is introduced only after deterministic baselines are trusted.

## 5) Relationship to other Email Intelligence docs

- `email-intelligence/PLANNING.md` defines the **Email Intelligence dashboard** target state
  (metadata-first, explainable clustering-first workflows, UI that never hides uncertainty).
- This document defines the broader **policy automation engine** target state.

These are complementary: the dashboard can remain the explainability surface that makes
policy actions feel justified, reversible, and safe.
