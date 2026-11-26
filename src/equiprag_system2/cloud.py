"""Lakeflow Jobs task dispatch. All side effects occur here, outside declarations."""

import argparse
import json
import time
from pathlib import Path

import yaml

from equiprag_system2.common.config_loader import (
    PIPELINES,
    EnrichmentSettings,
    load,
    pipeline_config,
)
from equiprag_system2.common.contract_views import install as install_contract_views
from equiprag_system2.common.delta import DeltaStore
from equiprag_system2.common.state import digest
from equiprag_system2.pipelines.gold_processing.transform import publish, reconcile
from equiprag_system2.pipelines.gold_processing.views import install
from equiprag_system2.pipelines.interaction_processing.transform import build
from equiprag_system2.pipelines.llm_enrichment.cloud import enrich, validate_results
from equiprag_system2.pipelines.llm_enrichment.delta_ledger import DeltaLedger
from equiprag_system2.pipelines.llm_enrichment.provenance import build_provenance
from equiprag_system2.pipelines.llm_enrichment.providers import (
    AzureProvider,
    DatabricksProvider,
    MockProvider,
    Provider,
    validate_databricks_endpoint,
)
from equiprag_system2.pipelines.silver_processing.cloud import refresh

TASKS = ("silver", "interactions", "requests", "enrich", "validate", "gold", "audit")


def execute(
    spark,
    *,
    root: Path,
    environment: str,
    task: str,
    run_id: str,
    landing_root: str,
    catalog: str,
    deployment: str,
    deployment_revision: str,
    endpoint: str,
    api_version: str,
    provider: str = "mock",
    manual_dev_writes: bool = False,
    databricks_token: str | None = None,
) -> None:
    if task not in TASKS:
        raise ValueError("unregistered task")
    env = {
        "ADLS_LANDING_ROOT": landing_root,
        "UC_CATALOG": catalog,
        "AZURE_OPENAI_DEPLOYMENT": deployment,
    }
    settings = load(root, environment, env)
    if manual_dev_writes:
        if environment != "dev":
            raise ValueError("Manual write opt-in is dev-only")
        settings = settings.model_copy(
            update={"processing": settings.processing.model_copy(update={"dry_run": False})}
        )
    if settings.processing.dry_run:
        raise ValueError("Dry-run environment: writes disabled; approve environment config first")
    if environment == "prod" and provider != "azure_openai":
        raise ValueError("Production requires Azure OpenAI")
    if provider == "databricks" and environment != "dev":
        raise ValueError("Databricks hosted inference is dev-only")
    for pipeline in PIPELINES:
        pipeline_config(root, pipeline, env)
    cfg = EnrichmentSettings.model_validate(
        {
            **pipeline_config(root, "llm_enrichment", env),
            "provider": provider,
            "model_deployment_revision": deployment_revision,
        }
    )
    if provider != "mock" and (not deployment_revision.strip() or deployment_revision == "mock-v1"):
        raise ValueError("Explicit approved live model revision required")
    if provider == "databricks":
        endpoint = validate_databricks_endpoint(endpoint)
        if not deployment.strip() or deployment == "mock":
            raise ValueError("Configure the hosted model identifier")
    store = DeltaStore(spark, settings)
    store.initialize()
    schema = json.loads((root / "pipelines/llm_enrichment/output_schema.json").read_text())
    taxonomy = yaml.safe_load((root / "pipelines/llm_enrichment/taxonomy_v1.yaml").read_text())
    prompt = (root / "prompts/enrich_interaction_v1.md").read_text()
    cfg = cfg.model_copy(
        update={
            "request_overhead_chars": len(prompt)
            + len(json.dumps(schema))
            + len(json.dumps(taxonomy))
        }
    )
    provenance = build_provenance(
        cfg, prompt, schema, taxonomy, endpoint=endpoint, api_version=api_version
    )
    started = time.time()
    try:
        if task == "silver":
            refresh(store, run_id)
        elif task == "interactions":
            for scope in store.get("worksets", run_id)["scopes"]:
                previous = store.get("episode_state", scope, {"interactions": []})
                store.put(
                    "episode_state",
                    scope,
                    build(store.get("conversation_state", scope), previous["interactions"]),
                    scope,
                )
        elif task == "requests":
            # Include all current episodes on version change, bounded and resumable by scope.
            previous = store.get("cursors", "enrichment_revision", {})
            work = store.get("worksets", run_id)
            if previous != provenance:
                work["scopes"] = sorted(
                    set(work["scopes"]) | {r["scope"] for r in store.rows("episode_state")}
                )
                store.put("worksets", run_id, work)
            for scope in work["scopes"]:
                episodes = store.get("episode_state", scope, {"interactions": []})
                DeltaLedger(store).reconcile(scope, episodes["interactions"], provenance)
            store.put("cursors", "enrichment_revision", provenance)
        elif task == "enrich":
            arguments = dict(
                endpoint=endpoint,
                deployment=deployment,
                schema=schema,
                prompt=prompt,
                taxonomy=taxonomy,
                max_output_tokens=cfg.max_output_tokens,
            )
            adapter: Provider
            if provider == "mock":
                adapter = MockProvider()
            elif provider == "databricks":
                adapter = DatabricksProvider(**arguments, api_key=databricks_token)
            else:
                adapter = AzureProvider(**arguments, api_version=api_version)
            enrich(store, cfg, adapter, run_id, provenance=provenance)
        elif task == "validate":
            validate_results(store, run_id, schema, taxonomy)
        elif task == "gold":
            for scope in store.get("worksets", run_id)["scopes"]:
                episodes = store.get("episode_state", scope, {"interactions": []})["interactions"]
                valid = {}
                for row in store.rows("requests", scopes=[scope]):
                    value = row["payload"]
                    if value["status"] == "VALIDATED":
                        i = value["input"]
                        valid[(i["interaction_id"], i["source_content_hash"])] = (
                            value["candidate"]["output"],
                            row["key"],
                        )
                gold = publish(store.get("conversation_state", scope), episodes, valid)
                # One atomic Delta row per conversation publishes the complete constellation.
                # Typed governed SQL views project facts and dimensions from this snapshot.
                gold["published_at"] = time.time()
                gold["silver_hash"] = digest(store.get("conversation_state", scope))
                store.put("publications", scope, gold, scope)
        elif task == "audit":
            install_contract_views(spark, settings)
            install(spark, settings)
            for scope in store.get("worksets", run_id)["scopes"]:
                reconcile(store.get("publications", scope))
            completed_scopes = set(store.get("worksets", run_id)["scopes"])
            for unfinished in store.unfinished_worksets():
                work = unfinished["payload"]
                if set(work["scopes"]) <= completed_scopes:
                    store.put("worksets", unfinished["key"], {**work, "completed": True})
        store.put(
            "runs",
            run_id + ":" + task,
            {
                "run_id": run_id,
                "task": task,
                "status": "SUCCEEDED",
                "duration_seconds": time.time() - started,
            },
        )
    except Exception:
        store.put(
            "runs",
            run_id + ":" + task,
            {"run_id": run_id, "task": task, "status": "FAILED", "error_category": "TASK_FAILED"},
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in (
        "root",
        "environment",
        "task",
        "run-id",
        "landing-root",
        "catalog",
        "deployment",
        "deployment-revision",
        "endpoint",
        "api-version",
        "provider",
    ):
        parser.add_argument("--" + name, required=True)
    args = vars(parser.parse_args())
    args["root"] = Path(args["root"])
    from pyspark.sql import SparkSession

    execute(SparkSession.builder.getOrCreate(), **args)


if __name__ == "__main__":
    main()
