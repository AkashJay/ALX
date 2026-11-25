"""Bounded concurrent inference; ledger operations stay on the coordinator thread."""

import random
import time
from concurrent.futures import ThreadPoolExecutor

from equiprag_system2.common.config_loader import EnrichmentSettings
from equiprag_system2.common.state import dumps
from equiprag_system2.pipelines.llm_enrichment.ledger import Ledger
from equiprag_system2.pipelines.llm_enrichment.providers import (
    Provider,
    ProviderError,
    Response,
    minimize,
)
from equiprag_system2.pipelines.llm_enrichment.validation import validate


def process(
    ledger: Ledger, provider: Provider, config: EnrichmentSettings, schema: dict, taxonomy: dict
) -> int:
    if not config.enabled:
        return 0
    processed, failures = 0, 0
    # Conservative per-run reservation; persisted rate budget survives hourly job retries.
    budget = ledger.state.read("llm_budget", {"minute": 0, "requests": 0, "tokens": 0})

    def call(claim: dict):
        supplied = minimize(claim["input"])
        if len(dumps(supplied)) > config.max_input_chars:
            return supplied, ProviderError("INPUT_TOO_LARGE"), 0
        start = time.monotonic()
        response: Response | ProviderError
        try:
            response = provider.generate(supplied)
        except ProviderError as exc:
            response = exc
        return supplied, response, time.monotonic() - start

    with ThreadPoolExecutor(max_workers=config.max_concurrency) as pool:
        while processed < config.request_batch_size and failures < config.max_attempts:
            minute = int(time.time() // 60)
            if budget["minute"] != minute:
                budget = {"minute": minute, "requests": 0, "tokens": 0}
            claims = []
            for _ in range(min(config.max_concurrency, config.request_batch_size - processed)):
                # UTF-8 byte bound is deliberately more conservative than token estimates.
                reservation = (
                    config.max_input_chars + config.request_overhead_chars
                ) * 4 + config.max_output_tokens
                if budget["requests"] >= config.requests_per_minute or (
                    budget["tokens"] + reservation > config.tokens_per_minute
                ):
                    break
                claim = ledger.claim(config.max_attempts, config.lease_seconds)
                if claim is None:
                    break
                claims.append(claim)
                budget["requests"] += 1
                budget["tokens"] += reservation
            with ledger.state.db:
                ledger.state.put("llm_budget", budget)
            if not claims:
                break
            for claim, (supplied, response, latency) in zip(claims, pool.map(call, claims)):
                if isinstance(response, ProviderError):
                    failures += 1
                    retry = response.retryable and claim["attempts"] < config.max_attempts
                    ledger.finish(
                        claim,
                        "RETRYABLE_FAILED" if retry else "PERMANENT_FAILED",
                        error=response.category,
                        delay=min(60, 2 ** claim["attempts"]) + random.random(),
                    )
                else:
                    errors = validate(response.candidate, supplied, schema, taxonomy)
                    ledger.finish(
                        claim,
                        "REVIEW_REQUIRED" if errors else "VALIDATED",
                        candidate={
                            "output": response.candidate,
                            "usage": response.usage,
                            "request_id": response.request_id,
                            "latency_seconds": latency,
                        },
                        errors=errors,
                    )
                processed += 1
    return processed
