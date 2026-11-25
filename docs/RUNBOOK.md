# EquipRAG System 2 runbook

**Current next step:** [Manual Databricks validation](MANUAL_VALIDATION.md). Jobs and bundle deployment are deferred until manual validation passes.

## Local synthetic execution
From the repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/pytest -q
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
.venv/bin/mypy src
.venv/bin/python -m equiprag_system2.local --state /tmp/equiprag-demo.sqlite
.venv/bin/python -m equiprag_system2.local --state /tmp/equiprag-demo.sqlite
```

The second invocation must have `ingested=0`, `processed=0`, and unchanged fact counts.
Use a fresh database for a clean demonstration; never reset a production checkpoint.
`--landing` accepts a synthetic directory. Supported layouts are `<feed>.jsonl` for
fixtures or `<feed>/**/*.jsonl` for additional immutable events. Write corrections in
new uniquely named files with a higher source_revision and a new event_id. The local
runner uses mock inference only. SQLite contains restricted raw data; keep it private.
Run one local coordinator at a time. Console output contains counts, IDs and timing only.

## Cloud setup (not yet executed)
The user confirmed Azure resources are not available and will be configured later.
No credentials, workspace or endpoint were tested. The bundle is a parameterized
candidate with static YAML tests; only the actual CLI/workspace can validate it.

1. Provision a serverless UC workspace, approved catalog and bronze/silver/enrichment/
   gold/ops schemas. Set UC external location/ADLS identity and immutable producer paths.
   Check regional endpoint approval, outbound connectivity and identity/token audience.
2. Approve production source contracts, taxonomy, turn semantics, data minimization,
   retention and quotas. Regex redaction is a development safeguard, **not an approved
   enterprise PII detector**. Production use remains gated on this approval.
3. Install a current Databricks CLI and configure a profile using enterprise identity.
   Set `BUNDLE_VAR_catalog`, `BUNDLE_VAR_landing_root` and use `--profile` where needed.
   No host, storage URI or secret is baked into this repository.
4. Build with `.venv/bin/python -m build --wheel --no-isolation`; run
   `databricks version` and `databricks bundle validate -t dev`. Verify wildcard wheel
   dependency resolution and the selected serverless environment against your CLI.
5. Review the target environment YAML and explicitly set `processing.dry_run: false`
   before cloud execution. Both ingestion and jobs refuse cloud writes while dry_run
   is true. Local synthetic writes remain permitted by the isolated local adapter.
6. Deploy the paused dev bundle, then manually run `hourly`. First use approved synthetic
   files and mock provider. Inspect Bronze CDF, independent feed checkpoints, quarantine,
   state, request history, serving views and scrubbed task audit.
7. For live inference, supply bundle variables `provider=azure_openai`, `deployment`,
   `deployment_revision`, `endpoint`, `api_version`. Install Azure SDK dependencies via
   the job environment and provision supported Entra credentials for DefaultAzureCredential.
   Serverless Azure workload identity availability is workspace-dependent; do not assume
   the compute automatically receives your ADLS identity. The SDK is configured for a
   60-second timeout with SDK retries disabled. Refusals are permanent ledger failures.
8. Complete the cloud acceptance checklist below before unpausing the hourly UTC schedule.
   Production rejects mock inference. Staging/production deployment identity, explicit
   bundle permissions and run-as principal must be supplied by the platform owner.

## State and replay
Bronze tables retain original JSONL text and file/ingestion metadata. The parser preserves
export fields, revision and rescued unknown fields in restricted receipts. Invalid records
with a known record_id block that identity (including historical valid state). This
conservative block requires a reviewed correction/remediation; merely adding a newer
version does not clear invalid history. Conflicting valid revisions recover on a higher
unambiguous revision. Deletes retain tombstones; missing parents reconcile when they arrive.
Conversation identity moves are rejected in cloud rather than silently changing ownership.

Cloud Silver consumes a fixed CDF version range. Persisted worksets precede cursor updates.
Unfinished worksets carry forward into later runs. Each affected conversation is rebuilt
from its restricted receipts. All driver collections are capped by max_records_per_batch;
exceeding a cap fails without dropping rows. This first implementation is bounded for a
pilot: a very large CDF commit or single-conversation history requires reviewed batch-limit
adjustment or a subsequent distributed processing extension. There is no automated
arbitrary-horizon backfill partitioner. Do not advance a cursor manually to bypass errors.
If CDF retention has expired, restore a retained checkpoint/snapshot and reconcile under
an approved recovery procedure; full source replay requires approved retention and spend.

The ledger key includes source hash and prompt/model/schema/taxonomy revisions plus artifact
hashes and transform/validator revisions. Only identical validated identities are reused.
Conditional lease tokens fence stale workers. Lease expiry permits recovery but cannot
prevent duplicate external API charges. Retries use bounded attempts and a durable
next_attempt timestamp; failed calls can retry on later hourly runs. RPM/TPM reservations
include input, output and prompt/schema/taxonomy overhead using conservative UTF-8 bounds.
One orchestrator owns the rate budget; multiple independent jobs writing the same tables
are unsupported. Request histories are retained and bounded reads can eventually require
an approved history archival policy. Retention deletion automation is not implemented.

## Publication and metrics
Gold uses one atomic Delta publication document per conversation, with typed fact,
dimension and bridge views (ADR-008), not separately materialized Delta fact tables.
Views exclude snapshots whose Silver state hash changed. Interaction views additionally
require a currently VALIDATED ledger identity. During replacement, metrics may temporarily
exclude affected conversations; unchanged conversations remain available. Restricted
publication documents and source/ledger tables are not BI grants.

Facts are current-active, not historical reporting snapshots. Message/conversation counts
are deterministic independently of inference. Interaction attributes are validated
candidate interpretations, never verified equipment outcomes. Clarification denominator
includes only non-null clarification flags; zero denominator yields null, not zero.
Topic counts use DISTINCT interaction IDs. Do not join message and topic bridges directly
and sum facts. Technician keys are hashed from already pseudonymous source identifiers;
this is not anonymization of real identities. Potential guidance concerns and resolution
assertions enter review. No automatic manual-review approval workflow is implemented.
The AI quality evaluation table is schema-only. Agent version is omitted without a
trustworthy source attribute. Publication timestamp is retained for freshness monitoring.

## Least privilege and retention
Provision ingestion and job writers with only their required UC privileges. Use a separate
view owner able to read underlying restricted tables. BI gets USE CATALOG/SCHEMA and SELECT
on approved semantic views only, never all tables in gold, silver or enrichment. Restrict
dim_technician and message-level views to approved roles. Verify access as a real BI principal.
Do not run blanket `GRANT SELECT ON SCHEMA`. No automatic grants are issued because
principal names and retention policy are not available. Set approved ADLS/Delta/CDF retention,
raw transcript retention and audit retention before production. Hard erasure requests need
an approved deletion policy covering source, history, candidates, backups and checkpoints.

## Cloud acceptance checklist (all pending)
- CLI bundle validate/build/deploy against actual version; verify wheel imports on serverless.
- Three source paths and independent checkpoints; restart with no duplicated new receipts.
- CDF replay, late revisions, edits, deletion, missing parents, malformed JSON, schema rescue.
- Crash after Silver cursor commit; next job recovers unfinished worksets and publishes.
- Competing stale claim token cannot persist after lease replacement or supersession.
- Endpoint structured outputs, Entra audience, refusal, 429, timeout and retry behavior.
- Gold typed view SQL and zero-denominator metrics execute in a SQL warehouse.
- BI principal cannot read raw text, publication documents, ledger or secrets.
- Measure representative throughput, max conversation size, token usage and freshness;
  agree alert thresholds and approved spend (none invented here).
- Verify dependency versions, retention policy, platform job permissions and run-as identity.

## Incident response
Use run/task IDs and reason categories. Inspect counts before restricted payloads. Repair
source contract failures with the source owner; never guess revisions. On endpoint outage,
leave validated unchanged data available and retry only pending identities. Quota exhaustion
leaves requests pending for later runs. On publication failure, keep the prior atomic
snapshot and repair the task; stale-snapshot guards prevent invalid derived eligibility.
Record the affected scope, lineage versions, recovery action and validation evidence.

## Official references consulted 2026-11-25
- [Bundle examples](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/examples)
- [Fixed-schema JSON parsing and Auto Loader text ingestion](https://learn.microsoft.com/en-us/azure/databricks/ldp/from-json-schema-evolution)
- [Azure structured outputs](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/structured-outputs)
Documentation review is not deployment validation. No preview schema inference is used.
