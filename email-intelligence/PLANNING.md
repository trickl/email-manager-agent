# Email Intelligence System — Target State & Planning Document

**Status:** North-star / target state (not an implementation guide)

The goal of this system is to provide deep, explainable insight into the structure, volume,
and nature of an entire email corpus, across all folders, labels, and historical messages.

The system is intentionally designed to:

- Operate initially on metadata-first ingestion
- Progressively introduce semantic understanding
- Minimise the need to read individual email bodies
- Enable high-confidence, low-effort actions (e.g. unsubscribe, archive, suppress)

This document describes the intended end-state of the system. It is not an implementation guide;
it is a north star against which future work can be evaluated.

## Conceptual Model (Mental Model for Users)

The user experience is modelled explicitly on disk space analysers (e.g. Baobab / WinDirStat /
DaisyDisk):

- Email is treated as a finite, inspectable resource
- Volume is immediately visible
- Hierarchies are navigable
- Large contributors are obvious
- Actions feel justified, not arbitrary

Instead of bytes on disk, the primary unit is:

- Number of emails

## Visual End State (Radial Hierarchy)

The primary dashboard visualisation is a radial, multi-level breakdown of the entire email corpus.

Hierarchy levels (outer → inner are navigable):

- Top-level category
  - e.g. Financial, Personal, Marketing, Work, System
- Sub-category
  - e.g. Financial → Receipts, Statements, Subscriptions
- Cluster / Campaign
  - Semantically similar emails (same newsletter, same notification pattern)
- Sender
  - Individual email address or domain

The radial chart encodes:

- Angle → number of emails
- Depth → categorisation level
- Colour → semantic category

The left-hand panel lists the same hierarchy numerically:

- Absolute count
- Percentage of total corpus
- Unread ratio
- Arrival frequency

## Data Ingestion End State

### Scope of Ingestion

The system ingests:

- Inbox
- Archived mail
- All folders / labels
- Historical email (full account history)

The ingestion model is incremental and idempotent:

- Initial full backfill
- Subsequent refreshes ingest only new or changed messages

The system assumes:

- The last known email state is correct
- Refresh operations are cheap and repeatable

## Core Data Layers (Target Architecture)

### Canonical Metadata Store (PostgreSQL)

Stores:

- Message identity
- Sender / recipients
- Subject + normalised subject
- Timestamps
- Read / unread state
- Folder / label membership

This layer is the source of truth.

### Semantic Index (Vector Database)

Stores:

- Semantic representations of email metadata (and later, bodies)
- Payload metadata for filtering

Primary responsibilities:

- Similarity search
- Cluster formation
- Pattern detection

This layer enables structure discovery, not truth.

### Analytics & Aggregation Layer

Derived data:

- Email frequency over time
- Read vs unread ratios
- Cluster sizes
- Sender dominance

This layer exists to:

- Make the UI fast
- Make agent reasoning cheap

## Clustering & Categorisation Strategy (Key Insight)

### Clustering First, Labelling Later

The system deliberately separates:

- Clustering (unsupervised, algorithmic)
- Labelling (semantic, LLM-assisted)

Clustering input signals:

- Normalised subject similarity
- Sender / domain
- Arrival frequency
- Temporal patterns

Each cluster should ideally contain:

- Tens to hundreds of emails

### Selective Body Inspection

To avoid reading thousands of emails:

- For each cluster, sample 3–4 representative email bodies
- Use these samples + frequency metadata
- Make one LLM call per cluster

This enables:

- Labelling clusters at scale
- Cost-bounded semantic understanding

## Label Taxonomy (Evolvable)

The Tier-1 taxonomy is **pre-seeded and enforced** (see `email-intelligence/TAXONOMY.md`) and is
also encoded in Postgres (`taxonomy_label`). There is intentionally no “Unknown” bucket; if a
cluster does not fit cleanly, a new taxonomical label must be created.

Initial top-level categories (illustrative, not fixed):

- Financial
  - Receipts
  - Statements
  - Subscriptions
- Marketing
  - Newsletters
  - Promotions
- Personal
- Work
- System / Automated

Labels are:

- Stored separately from raw data
- Versioned
- Re-assignable as models improve

## Actions Enabled by the System

Once labelled, the system enables:

- Suggested unsubscribe actions
- Bulk archive recommendations
- Suppression of low-value notifications
- Highlighting of high-signal senders

All actions are:

- Explainable
- Reversible
- Confidence-scored

## Dashboard Functional Requirements

The UI must:

- Load without data
- Clearly show ingestion state
- Offer explicit actions:
  - Ingest all mail
  - Refresh since last sync
- Allow drilling from macro → micro
- Never hide uncertainty

## Success Criteria (Target State)

The system is successful when:

- Tens of thousands of emails can be understood via a few hundred clusters
- The user rarely needs to read raw email bodies
- Large, low-value email sources are visually obvious
- Unsubscribe / archive decisions feel obvious, not risky

## Guiding Principle

The system does not decide for the user. It makes the structure of their inbox impossible to ignore.

## Relationship to README

Once this target state is accepted:

- A condensed version becomes the project README
- This document remains the long-form architectural intent
- Implementation should be judged against it
