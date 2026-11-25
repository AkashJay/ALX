"""Incremental CDF receipt processing and bounded affected-conversation rebuilds."""

import json

from equiprag_system2.common.config_loader import SOURCES, table_name
from equiprag_system2.common.delta import DeltaStore
from equiprag_system2.contracts.events import validate_event
from equiprag_system2.pipelines.silver_processing.transform import canonicalize


def refresh(store: DeltaStore, run_id: str) -> list[str]:
    from pyspark.sql import functions as F

    # Persist work before advancing cursors, so task retries cannot lose invalidations.
    work = store.get("worksets", run_id, {"scopes": [], "ends": {}})
    scopes = set(work["scopes"])
    for unfinished in store.unfinished_worksets():
        scopes.update(unfinished["payload"]["scopes"])
    ends = dict(work["ends"])
    for source in SOURCES:
        table = table_name(store.settings, "bronze", source + "_raw")
        end = int(store.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
        cursor = store.get("cursors", source, {"version": -1})["version"]
        if end <= cursor:
            continue
        frame = (
            store.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", cursor + 1)
            .option("endingVersion", end)
            .table(table)
            .where(F.col("_change_type") == "insert")
        )
        rows = frame.limit(store.limit + 1).collect()
        if len(rows) > store.limit:
            raise ValueError("BRONZE_BATCH_EXCEEDED: bounded replay required; cursor unchanged")
        for row in rows:
            try:
                decoded = json.loads(row.raw)
                payload, rescued, reason = validate_event(source, decoded)
            except (ValueError, TypeError):
                decoded, payload, rescued, reason = {}, None, {}, "MALFORMED_JSON"
            scope = decoded.get("conversation_id", "") if isinstance(decoded, dict) else ""
            if not isinstance(scope, str):
                scope = ""
            # Immutable entity ownership: moving an existing source record to a new
            # conversation invalidates both scopes, rather than leaving a stale old copy.
            if scope and isinstance(decoded, dict) and isinstance(decoded.get("record_id"), str):
                prior = store.spark.table(store.name("receipts")).where(
                    (F.get_json_object("payload", "$.source") == source)
                    & (F.get_json_object("payload", "$.decoded.record_id") == decoded["record_id"])
                )
                owners = prior.select("scope").distinct().limit(store.limit + 1).collect()
                if len(owners) > store.limit:
                    raise ValueError("OWNER_HISTORY_LIMIT_EXCEEDED")
                for owner in owners:
                    if owner.scope and owner.scope != scope:
                        reason = "SOURCE_RECORD_MOVED"
                        scopes.add(owner.scope)
                        store.put(
                            "receipts",
                            row.receipt_id + ":blocked:" + owner.scope,
                            {
                                "id": row.receipt_id,
                                "source": source,
                                "payload": None,
                                "decoded": decoded,
                                "reason": reason,
                            },
                            owner.scope,
                        )
            # Minimal key tombstones can recover the owning conversation from history.
            if not scope and isinstance(decoded, dict) and decoded.get("record_id"):
                prior = store.spark.table(store.name("receipts")).where(
                    (F.get_json_object("payload", "$.source") == source)
                    & (F.get_json_object("payload", "$.decoded.record_id") == decoded["record_id"])
                )
                owners = prior.select("scope").distinct().limit(2).collect()
                if len(owners) == 1:
                    scope = owners[0].scope
            receipt = {
                "id": row.receipt_id,
                "source": source,
                "payload": payload,
                "decoded": decoded,
                "rescued": rescued,
                "reason": reason,
                "source_filename": row.source_filename,
                "ingested_at": row.ingested_at.isoformat(),
            }
            store.put("receipts", row.receipt_id, receipt, scope)
            if scope:
                scopes.add(scope)
        ends[source] = end
    store.put("worksets", run_id, {"scopes": sorted(scopes), "ends": ends})
    for scope in sorted(scopes):
        receipts = [r["payload"] for r in store.rows("receipts", scopes=[scope])]
        store.put("conversation_state", scope, canonicalize(receipts), scope)
    for source, end in ends.items():
        store.put("cursors", source, {"version": end})
    return sorted(scopes)
