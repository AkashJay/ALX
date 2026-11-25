"""Order-independent latest state, ambiguity blocking, tombstones and link integrity."""

from collections import defaultdict

from equiprag_system2.common.config_loader import SOURCES
from equiprag_system2.common.state import digest
from equiprag_system2.contracts.events import KEYS, utc_timestamp


def canonicalize(events: list[dict]) -> dict:
    groups: dict = defaultdict(list)
    rejected = []
    blocked = set()
    for receipt in events:
        if receipt.get("reason"):
            decoded = receipt.get("decoded", {})
            if isinstance(decoded, dict) and isinstance(decoded.get("record_id"), str):
                blocked.add((receipt["source"], decoded["record_id"]))
            rejected.append({"receipt_id": receipt["id"], "reason": receipt["reason"]})
        else:
            groups[(receipt["source"], receipt["payload"]["record_id"])].append(receipt["payload"])
    event_ids: dict = defaultdict(list)
    for (source, record_id), versions in groups.items():
        for event in versions:
            if event.get("event_id"):
                event_ids[(source, event["event_id"])].append((record_id, event))
    for (source, _), occurrences in event_ids.items():
        if (
            len(
                {
                    digest(
                        {k: v for k, v in e.items() if k not in ("exported_at", "export_batch_id")}
                    )
                    for _, e in occurrences
                }
            )
            > 1
        ):
            blocked.update((source, record_id) for record_id, _ in occurrences)
    current: dict = {name: {} for name in SOURCES}
    tombstones = []
    for (source, record_id), versions in sorted(groups.items()):
        if (source, record_id) in blocked:
            rejected.append(
                {"source": source, "record_id": record_id, "reason": "INVALID_HISTORY_BLOCK"}
            )
            continue
        revision = max(v["source_revision"] for v in versions)
        latest = [v for v in versions if v["source_revision"] == revision]

        # Export metadata is receipt-level and is not a source-content conflict.
        def semantic(v: dict) -> str:
            return digest(
                {
                    k: x
                    for k, x in v.items()
                    if k not in ("exported_at", "export_batch_id", "event_id")
                }
            )

        if len({semantic(v) for v in latest}) != 1:
            rejected.append(
                {"source": source, "record_id": record_id, "reason": "AMBIGUOUS_REVISION"}
            )
            continue
        event = latest[0]
        if len({v[KEYS[source]] for v in versions}) != 1:
            rejected.append({"source": source, "record_id": record_id, "reason": "KEY_CHANGED"})
            continue
        if event["operation"] == "DELETE":
            tombstones.append({"source": source, **event})
            if not any(
                v["operation"] == "UPSERT" and v["source_revision"] < revision for v in versions
            ):
                rejected.append(
                    {"source": source, "record_id": record_id, "reason": "UNKNOWN_DELETE"}
                )
            continue
        key = event[KEYS[source]]
        if key in current[source]:
            current[source][key] = None  # Different record IDs claiming one entity block both.
        else:
            current[source][key] = event
    for source in SOURCES:
        for key, event in list(current[source].items()):
            reason = None
            if event is None:
                reason = "DUPLICATE_ENTITY_KEY"
            elif source != "conversations":
                parent = current["conversations"].get(event["conversation_id"])
                if not parent:
                    reason = "MISSING_PARENT"
                elif utc_timestamp(event["created_at"]) < utc_timestamp(parent["started_at"]):
                    reason = "BEFORE_CONVERSATION"
                elif source == "feedback" and event.get("message_id"):
                    message = current["messages"].get(event["message_id"])
                    if not message or message["conversation_id"] != event["conversation_id"]:
                        reason = "MISSING_MESSAGE_PARENT"
            if reason:
                rejected.append({"source": source, "entity_id": key, "reason": reason})
                del current[source][key]
    return {
        "active": {s: list(rows.values()) for s, rows in current.items()},
        "tombstones": tombstones,
        "rejected": rejected,
    }
