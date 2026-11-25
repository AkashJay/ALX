# EquipRAG System 2 — manual Databricks validation

This branch contains the Python package and supporting files for manual Azure Databricks
Serverless validation. Jobs, CI and bundle deployment are deferred.

Start with [the manual validation guide](docs/MANUAL_VALIDATION.md) and open
[`notebooks/manual/validate_pipeline.py`](notebooks/manual/validate_pipeline.py) in your
Databricks Git folder. Install the package from the cloned repository, configure the
non-secret notebook widgets, and run each stage in order with the mock LLM first.

## Included

- `src/equiprag_system2/`: shared Python package, pipeline logic and adapters.
- `pyproject.toml`: package installation and dependencies.
- `config/`: safe defaults and environment templates; no Azure credentials.
- `pipelines/`: pipeline behavior, source contracts, output schema and taxonomy.
- `prompts/`: versioned enrichment prompt.
- `notebooks/manual/`: thin manual execution notebook.
- `samples/landing/`: synthetic inputs for the initial validation.
- `docs/`: manual guide, operational runbook and architecture decisions.

The package keeps its local/mock adapters so its existing entrypoints and imports remain
intact. Local tests and developer scaffolding are not included in this manual deployment
branch. The local implementation passed 42 tests, lint and type checks before publication;
actual Azure execution is still unvalidated.

## Execution

Prepare a dedicated dev catalog and approved ADLS locations. Upload the synthetic inputs
under their feed directories as described in the guide. In the notebook, configure the
repository path, catalog, landing location, durable checkpoint root and run ID. Explicitly
set `allow_writes=true` to enable dev writes.

Run Bronze → Silver → interactions → requests → enrichment → validation → Gold → audit.
Repeat with the same checkpoints to validate replay before testing source edits/deletions.
Credentials belong in approved identity/secret facilities, never Git or notebook widgets.

Gold publishes evidence-validated candidate interpretations, not verified equipment safety
or repair outcomes. See [the runbook](docs/RUNBOOK.md) for known limits and cloud acceptance
checks. Automation resources referenced there will be added in a later deployment phase.
