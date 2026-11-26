# Free Edition setup for this validation

Use the existing `workspace` catalog and the five `equiprag_` schemas. The managed-volume
landing and checkpoint paths are entered through widgets; no Azure account or live LLM
is needed for this mock test. The manual adapter accepts `/Volumes/` paths. The production
Lakeflow adapter still requires ADLS, as specified by the Azure architecture.

Example widget values for the resources created in this walkthrough:

| Widget | Value |
|---|---|
| catalog | workspace |
| landing_root | /Volumes/workspace/equiprag_bronze/equiprag_landing |
| checkpoint_root | /Volumes/workspace/equiprag_ops/equiprag_checkpoints |
| provider | mock |

These are documentation examples; runtime configuration remains in the notebook widgets.
Do not run yet until the repo is pulled, package installed and synthetic files uploaded.

# Manual Databricks validation — before jobs or bundles

This is the current development path. No job creation, scheduling, bundle deployment or
live LLM is required for the first cloud validation. A Databricks workspace is still
required; local tests do not establish cloud compatibility.

1. Push code and synthetic samples to GitHub and clone into a Databricks Git folder.
   Do not commit credentials, real transcripts, widget exports or output-bearing notebooks.
2. Use a dev catalog with equiprag_bronze, equiprag_silver, equiprag_enrichment,
   equiprag_gold and equiprag_ops schemas (the dev configuration maps to these names).
   Your notebook identity needs the approved UC/storage privileges. Use a notebook with
   serverless compute supporting Auto Loader AvailableNow and Delta CDF. Do not mix this
   manual ingestion with a Lakeflow pipeline writing the same targets.
3. Upload synthetic samples as completed immutable files beneath the managed-volume or ADLS landing root:
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
   - `landing_root`: `/Volumes/<catalog>/<schema>/<volume>` or an approved abfss URI,
     without credentials/query parameters.
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
7. After audit succeeds, inspect `<catalog>.equiprag_gold.vw_usage_daily`,
   `vw_interaction_topics`, `vw_clarification_metrics`, and the fact views in Databricks SQL.
   Initial fixtures should produce 1 conversation, 2 messages and 1 validated interaction.
   Clarification is unknown: denominator zero and rate null. Inspect restricted
   `<catalog>.equiprag_ops.runs` for stage status and restricted rejection views for reason codes.
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

## Optional Databricks hosted model — dev only

After verifying a strict JSON smoke response in the workspace, install the package with
`%pip install "/Workspace/<your Git folder path>[inference]"` and restart Python.
The manual notebook passes `dbutils` to `ManualSession`; authentication is resolved only
at enrichment from `DATABRICKS_TOKEN` or the notebook context. Never print tokens or put
them in widgets/config/Git. If notebook context authentication is unavailable, configure
the environment through an approved secret mechanism. No Azure identity is needed for
this dev adapter; production still requires Azure OpenAI.

Set provider=`databricks`, deployment to the exact model identifier shown by Playground
(e.g. `system.ai.gpt-oss-120b`), endpoint to the workspace's generated HTTPS
`/ai-gateway/mlflow/v1` base URL, deployment_revision to an explicit evaluation label
(e.g. `gpt-oss-120b-eval-v1`). The label is not an immutable hosted model version guarantee.
The api_version widget is unused for this provider. Use a fresh run_id and keep the same
landing/checkpoint roots. Leave the pipeline prompt, taxonomy and output schema intact.

Create a fresh session and run Bronze, Silver, interactions and requests **before** enrich.
The expanded provenance creates new request identities and supersedes prior mock requests;
source data does not need to be copied again. The first replay after this code update also
changes existing identities once, even if continuing with mock. Do not remove checkpoints
or request history. Only one writer may operate the target.

Run enrich, then inspect candidate/status/attempts; run validate and inspect validation
reasons before Gold. A normal completion and valid JSON do not prove evidence validity.
Real outputs can enter REVIEW_REQUIRED. Confirm the full schema is accepted in the actual
workspace; the earlier equipment-only smoke test did not test that contract. Run Gold and
audit only after reviewing those outcomes, then verify counts and unchanged replay.
