"""Bounded cloud worker; durable rate budget, leased work, no Spark inference UDFs."""

import random
import time
from concurrent.futures import ThreadPoolExecutor

from equiprag_system2.common.config_loader import EnrichmentSettings
from equiprag_system2.common.delta import DeltaStore
from equiprag_system2.common.state import dumps
from equiprag_system2.pipelines.llm_enrichment.delta_ledger import DeltaLedger
from equiprag_system2.pipelines.llm_enrichment.providers import ProviderError, minimize
from equiprag_system2.pipelines.llm_enrichment.validation import validate


def pending(store: DeltaStore) -> list[dict]:
    import json

    from pyspark.sql import functions as F

    frame = store.spark.table(store.name("requests")).where(
        F.get_json_object("payload", "$.status").isin("PENDING", "PROCESSING", "RETRYABLE_FAILED")
    )
    rows = frame.orderBy("key").limit(store.limit).collect()
    return [
        {"key": r.key, "scope": r.scope, "payload": json.loads(r.payload), "token": r.token}
        for r in rows
    ]


def enrich(store: DeltaStore, cfg: EnrichmentSettings, provider, run_id: str) -> int:
    if not cfg.enabled:
        return 0
    ledger = DeltaLedger(store)
    work = store.get("worksets", run_id, {"scopes": [], "ends": {}})
    budget = store.get("budgets", "inference", {"minute": 0, "requests": 0, "tokens": 0})
    processed, failures = 0, 0
    queue = pending(store)[: cfg.request_batch_size]

    def call(row):
        supplied = minimize(row["payload"]["input"])
        start = time.monotonic()
        if len(dumps(supplied)) > cfg.max_input_chars:
            return ProviderError("INPUT_TOO_LARGE"), 0
        try:
            return provider.generate(supplied), time.monotonic() - start
        except ProviderError as error:
            return error, time.monotonic() - start

    with ThreadPoolExecutor(max_workers=cfg.max_concurrency) as pool:
        for offset in range(0, len(queue), cfg.max_concurrency):
            claims = []
            minute = int(time.time() // 60)
            if budget["minute"] != minute:
                budget = {"minute": minute, "requests": 0, "tokens": 0}
            for row in queue[offset : offset + cfg.max_concurrency]:
                reserve = (
                    cfg.max_input_chars + cfg.request_overhead_chars
                ) * 4 + cfg.max_output_tokens
                if budget["requests"] >= cfg.requests_per_minute or (
                    budget["tokens"] + reserve > cfg.tokens_per_minute
                ):
                    break
                claim = ledger.claim(row, cfg.max_attempts, cfg.lease_seconds)
                if claim:
                    claims.append(claim)
                    budget["requests"] += 1
                    budget["tokens"] += reserve
            # Single coordinator owns rate allocation; budget is committed before calls.
            store.put("budgets", "inference", budget)
            work["scopes"] = sorted(set(work["scopes"]) | {r["scope"] for r in claims})
            store.put("worksets", run_id, work)
            for row, (response, latency) in zip(claims, pool.map(call, claims)):
                if isinstance(response, ProviderError):
                    retry = response.retryable and row["payload"]["attempts"] < cfg.max_attempts
                    ledger.finish(
                        row,
                        "RETRYABLE_FAILED" if retry else "PERMANENT_FAILED",
                        error=response.category,
                        delay=min(60, 2 ** row["payload"]["attempts"]) + random.random(),
                    )
                    failures += 1
                else:
                    ledger.finish(
                        row,
                        "SUCCEEDED",
                        candidate={
                            "output": response.candidate,
                            "usage": response.usage,
                            "request_id": response.request_id,
                            "latency_seconds": latency,
                        },
                    )
                processed += 1
            if failures >= cfg.max_attempts:
                break
    return processed


def validate_results(store: DeltaStore, run_id: str, schema: dict, taxonomy: dict) -> None:
    ledger = DeltaLedger(store)
    for scope in store.get("worksets", run_id)["scopes"]:
        for row in store.rows("requests", scopes=[scope]):
            value = row["payload"]
            if value["status"] != "SUCCEEDED":
                continue
            errors = validate(
                value["candidate"]["output"], minimize(value["input"]), schema, taxonomy
            )
            value["validation"] = errors
            ledger._transition(value, "REVIEW_REQUIRED" if errors else "VALIDATED")
            store.compare_swap("requests", row["key"], row["token"], value)
