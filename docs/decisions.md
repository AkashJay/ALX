# Architecture Decision Log

- ADR-001 — Adopt Azure Databricks Serverless, Lakeflow Spark Declarative Pipelines and Lakeflow Jobs hourly UTC for v1.
- ADR-002 — Separate `pipelines/` (contracts/config), `src/` (business logic), `notebooks/` (thin entrypoints), `resources/` (deployment).
- ADR-003 — Use immutable ADLS JSONL exports and Auto Loader for Bronze; explicit revision-based Silver current state.
- ADR-004 — Use bounded Azure OpenAI Python adapter for versioned evidence-linked enrichment. Publish validated candidates only.
- ADR-005 — Use Bronze/Silver/Gold Delta in Unity Catalog with separate conversation/message/interaction fact grains.

Open decisions are in `docs/OPEN_DECISIONS.md`; these are agreed development defaults, not verified live source contracts.

## ADR-006 — Executable synthetic contract and local state
Synthetic fixtures explicitly carry positive source_revision and event_id. Correct the
illustrative message schema sequence_no to integer. Require revisions for this adapter;
missing revision is quarantined rather than inventing ordering. Conflicting payloads at
the same revision block the entity until a newer unambiguous revision arrives.
SQLite is the durable local test adapter only, never a substitute for cloud Delta.
Imperative Silver jobs are selected because ambiguous revisions must block publication;
managed AUTO CDC alone does not implement that policy.

## ADR-007 — Current-active publication and conservative semantics
Gold represents current active entities. A changed or deleted source immediately removes
stale derived eligibility, retaining restricted historical candidates. An unchanged valid
interaction remains available during unrelated inference outages. Unknown boolean claims
use null (the initial schema incorrectly forced unknown to false); true and false both
require evidence. Potential guidance issues and resolution assertions enter review, never
automatically become correctness claims. Only explicitly supported mock labels are emitted.
Fallback segmentation accepts strictly alternating user/assistant pairs; complex or partial
turns are quarantined. Production turn reliability remains a source-owner decision.

## ADR-008 — Bounded cloud coordinator and atomic serving snapshots
Initial cloud job reads Bronze Delta CDF and rebuilds only affected conversations, with
hard configured collection bounds. Exceeding a bound fails before cursor advancement;
this is a bounded pilot implementation, not a demonstrated high-volume distributed engine.
Restricted intermediate Delta documents retain receipts, canonical state and request
histories. One atomic publication row per conversation contains the fact constellation;
typed Gold SQL views expose its explicit grains, dimensions and bridges. This avoids
pretending multi-table Delta writes are atomic. Views suppress publications whose Silver
hash is stale and interactions whose request is no longer validated. Physical separately
materialized fact tables are deferred until a measured workload requires them. No claim
of full production readiness follows from local tests. Single scheduled job is the only
writer; multiple independent orchestrators are unsupported. SDK remote retries are disabled;
ledger controls attempts. Failed calls retry on later runs after exponential backoff.

## ADR-009 — Manual cloud validation before automation
User requests notebook-driven validation before jobs/bundles. Add an imperative Auto Loader
AvailableNow adapter sharing the Bronze reader with the declarative implementation, with
explicit durable per-feed checkpoints. Manual runtime settings are typed and supplied in
notebook widgets, never committed credentials. An explicit dev-only write opt-in overrides
dry_run for this manual session; normal job configuration remains unchanged. Do not run
manual and declarative ingestion against the same targets/checkpoints concurrently.
