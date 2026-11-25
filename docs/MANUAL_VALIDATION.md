# Manual Databricks validation — before jobs or bundles

This is the current development path. No job creation, scheduling, bundle deployment or
live LLM is required for the first cloud validation. Azure/Databricks access is still
required; local tests do not establish cloud compatibility.

1. Push code and synthetic samples to GitHub and clone into a Databricks Git folder.
   Do not commit credentials, real transcripts, widget exports or output-bearing notebooks.
2. Prepare a dedicated dev catalog with bronze, silver, enrichment, gold and ops schemas.
   Your notebook identity needs the approved UC/storage privileges. Use a notebook with
   serverless compute supporting Auto Loader AvailableNow and Delta CDF. Do not mix this
   manual ingestion with a Lakeflow pipeline writing the same targets.
3. Upload synthetic samples as completed immutable files beneath the ADLS landing root:
   `conversations/initial.jsonl`, `messages/initial.jsonl`, `feedback/initial.jsonl`.
   These are the contents of the corresponding files in `samples/landing/`.
4. Open `notebooks/manual/validate_pipeline.py`. Before its import cell, run a notebook
   cell such as `%pip install "/Workspace/<your Git folder path>"`. Substitute the actual
   repository root. If needed, run `dbutils.library.restartPython()` in a separate cell.
   For later live inference install `"/Workspace/<your Git folder path>[azure]"` instead.
   Dependency downloads need approved network access; the package contains all Python
   logic, while configuration/prompt files are read from the repository root widget.
5. Run the widget creation cell. Enter:
   - `root`: absolute workspace repository root containing config/, pipelines/ and prompts/.
   - `catalog`: dedicated dev UC catalog (must already exist).
   - `landing_root`: approved abfss URI, without credentials/query parameters.
   - `checkpoint_root`: persistent `/Volumes/<catalog>/<schema>/<volume>/manual-state`
     or approved abfss directory outside the landing root. Create/access it beforehand.
   - `run_id`: a new identifier such as `manual-001` for this complete validation pass.
   - `allow_writes`: `true` when ready to write dev tables. Default false blocks writes.
   - Leave `provider=mock` and the remaining model settings at their defaults initially.
6. Run the configuration cell, then each stage cell in order:
   Bronze → Silver → interactions → requests → enrich → validate → Gold → audit.
   The package enforces ordering. If a stage fails, fix the cause and rerun that cell;
   completed stages are retained in the notebook session. After a Python restart, create
   a fresh session and replay all stages; durable receipts, leases and worksets handle
   recovery. Never delete checkpoints to work around errors.
7. After audit succeeds, inspect `<catalog>.gold.vw_usage_daily`,
   `vw_interaction_topics`, `vw_clarification_metrics`, and the fact views in Databricks SQL.
   Initial fixtures should produce 1 conversation, 2 messages and 1 validated interaction.
   Clarification is unknown: denominator zero and rate null. Inspect restricted
   `<catalog>.ops.runs` for stage status and restricted rejection views for reason codes.
   A successful stage alone does not prove source acceptance; check quarantine and counts.
8. Set a fresh run_id, recreate the session and repeat all stages with the same checkpoint
   root. Facts must remain unchanged and no new validated request identity should appear.
   Next add a higher-revision edit/delete in a new immutable file and verify invalidation.

The dev write opt-in is scoped to this session. It does not change committed YAML or enable
job execution. Checkpoint paths are stable per feed and target: use a separate root when
changing the catalog or landing dataset; retain the same root for replay of the same target.
Only one notebook/orchestrator may write a given dev target at a time.

For live inference later, configure provider, deployment, deployment_revision, endpoint and
api_version widgets, and provision approved Entra authentication outside the notebook.
Widgets are not secret storage. Follow RUNBOOK.md's identity, privacy and source approval gates.

Manual runs use the same shared Bronze reader and downstream package functions as the
future automated workflow. A finite AvailableNow stream replaces the declarative registration
only for this validation path (ADR-009). Supported trigger/checkpoint references:
[serverless streaming](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/streaming)
and [streaming checkpoints](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/checkpoints).
Real serverless notebook execution, Spark SQL and access control remain unvalidated until
this checklist runs in your workspace. Jobs and bundle rollout come after this validation.
