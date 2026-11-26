"""Runnable synthetic end-to-end adapter; never connects to Azure implicitly."""

import argparse
import json
import time
import uuid
from pathlib import Path

import yaml

from equiprag_system2.common.config_loader import (
    PIPELINES,
    EnrichmentSettings,
    load,
    pipeline_config,
)
from equiprag_system2.common.state import State, dumps
from equiprag_system2.pipelines.conversation_ingestion.local import ingest
from equiprag_system2.pipelines.gold_processing.transform import publish
from equiprag_system2.pipelines.interaction_processing.transform import build
from equiprag_system2.pipelines.llm_enrichment.ledger import Ledger
from equiprag_system2.pipelines.llm_enrichment.provenance import build_provenance
from equiprag_system2.pipelines.llm_enrichment.providers import MockProvider, Provider
from equiprag_system2.pipelines.llm_enrichment.worker import process
from equiprag_system2.pipelines.silver_processing.transform import canonicalize


def _decode(raw: str):
    try:
        return json.loads(raw)
    except ValueError:
        return {}


def run(
    root: Path,
    landing: Path,
    state: State,
    provider: Provider | None = None,
    *,
    prompt_version: str | None = None,
) -> dict:
    env = {
        "ADLS_LANDING_ROOT": str(landing.resolve()),
        "UC_CATALOG": "synthetic",
        "AZURE_OPENAI_DEPLOYMENT": "mock",
    }
    settings = load(root, "dev", env)
    configs = {name: pipeline_config(root, name, env) for name in PIPELINES}
    cfg = EnrichmentSettings.model_validate(
        {
            **configs["llm_enrichment"],
            "provider": "mock",
            "model_deployment_revision": "mock-v1",
            **({"prompt_version": prompt_version} if prompt_version else {}),
        }
    )
    schema = json.loads((root / "pipelines/llm_enrichment/output_schema.json").read_text())
    taxonomy = yaml.safe_load((root / "pipelines/llm_enrichment/taxonomy_v1.yaml").read_text())
    prompt = (root / "prompts/enrich_interaction_v2.md").read_text()
    cfg = cfg.model_copy(
        update={
            "request_overhead_chars": len(prompt)
            + len(json.dumps(schema))
            + len(json.dumps(taxonomy))
        }
    )
    provenance = build_provenance(cfg, prompt, schema, taxonomy)
    run_id, start = str(uuid.uuid4()), time.time()
    try:
        ingested = ingest(state, Path(settings.storage.landing_root), root)
        receipts = [
            {**dict(r), "payload": json.loads(r["payload"]), "decoded": _decode(r["raw"])}
            for r in state.db.execute("SELECT * FROM bronze ORDER BY id")
        ]
        silver = canonicalize(receipts)
        episodes = build(silver, state.read("interactions", []))
        ledger = Ledger(state)
        ledger.reconcile(episodes["interactions"], provenance)
        processed = process(ledger, provider or MockProvider(), cfg, schema, taxonomy)
        valid = {
            (r["interaction_id"], r["content_hash"]): (
                json.loads(r["candidate"])["output"],
                r["key"],
            )
            for r in state.db.execute("SELECT * FROM requests WHERE status='VALIDATED'")
        }
        gold = publish(silver, episodes["interactions"], valid)
        audit = {
            "run_id": run_id,
            "status": "SUCCEEDED",
            "ingested": ingested,
            "bronze_count": len(receipts),
            "rejected": len(silver["rejected"]) + len(episodes["rejected"]),
            "processed": processed,
            "changed_interactions": len(episodes["changed"]),
            "metrics": gold["metrics"],
            "completed_at": time.time(),
            "duration_seconds": time.time() - start,
        }
        # One transaction exposes a consistent snapshot of all local layers.
        with state.db:
            for name, value in [
                ("silver", silver),
                ("interactions", episodes["interactions"]),
                ("interaction_message_bridge", episodes["bridge"]),
                ("changed_interaction_keys", episodes["changed"]),
                ("interaction_rejections", episodes["rejected"]),
                ("gold", gold),
            ]:
                state.put(name, value)
            state.db.execute("INSERT INTO runs VALUES (?,?)", (run_id, dumps(audit)))
        return audit
    except Exception:
        with state.db:
            state.db.execute(
                "INSERT INTO runs VALUES (?,?)",
                (
                    run_id,
                    dumps(
                        {"run_id": run_id, "status": "FAILED", "error_category": "PIPELINE_FAILED"}
                    ),
                ),
            )
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--landing", type=Path)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    state = State(args.state)
    try:
        print(dumps(run(args.root, args.landing or args.root / "samples/landing", state)))
    finally:
        state.close()


if __name__ == "__main__":
    main()
