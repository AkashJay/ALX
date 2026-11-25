"""Current-active constellation and fanout-safe aggregate metrics."""

from collections import Counter

from equiprag_system2.common.state import digest


def publish(silver: dict, interactions: list[dict], validated: dict) -> dict:
    active = silver["active"]
    facts = []
    for interaction in interactions:
        candidate = validated.get(
            (interaction["interaction_id"], interaction["source_content_hash"])
        )
        if candidate is None:
            continue
        output, request_key = candidate
        facts.append(
            {
                "interaction_id": interaction["interaction_id"],
                "source_content_hash": interaction["source_content_hash"],
                "conversation_id": interaction["conversation_id"],
                "date_key": interaction["created_at"][:10],
                "request_key": request_key,
                **{
                    k: output[k]
                    for k in (
                        "topic",
                        "intent",
                        "equipment",
                        "component",
                        "clarification_requested",
                        "follow_up_present",
                        "resolution_confirmed_by_conversation",
                    )
                },
            }
        )
    messages = [
        {
            "message_id": m["message_id"],
            "conversation_id": m["conversation_id"],
            "role": m["role"],
            "date_key": m["created_at"][:10],
        }
        for m in active["messages"]
    ]
    message_counts = Counter(m["conversation_id"] for m in messages)
    interaction_counts = Counter(i["conversation_id"] for i in facts)
    conversations = [
        {
            "conversation_id": c["conversation_id"],
            "date_key": c["started_at"][:10],
            "technician_key": digest(["technician", c["technician_id"]])
            if c.get("technician_id")
            else None,
            "channel": c["channel"],
            "message_count": message_counts[c["conversation_id"]],
            "interaction_count": interaction_counts[c["conversation_id"]],
        }
        for c in active["conversations"]
    ]
    result: dict = {
        "fact_conversation": conversations,
        "fact_message": messages,
        "fact_interaction": facts,
    }
    for field in ("topic", "intent", "equipment"):
        result["dim_" + field] = [
            {field + "_key": digest([field, v]), "label": v}
            for v in sorted({f[field] for f in facts if f[field] is not None})
        ]
        if field != "intent":
            result["bridge_interaction_" + field] = [
                {"interaction_id": f["interaction_id"], field + "_key": digest([field, f[field]])}
                for f in facts
                if f[field] is not None
            ]
    result["dim_date"] = [
        {"date_key": d}
        for d in sorted({r["date_key"] for rows in (conversations, messages, facts) for r in rows})
    ]
    result["dim_technician"] = [
        {"technician_key": t}
        for t in sorted({c["technician_key"] for c in conversations if c["technician_key"]})
    ]
    eligible = [f for f in facts if f["clarification_requested"] is not None]
    numerator = sum(f["clarification_requested"] is True for f in eligible)
    result["metrics"] = {
        "conversation_count": len(conversations),
        "message_count": len(messages),
        "validated_interaction_count": len(facts),
        "eligible_interaction_count": len(eligible),
        "clarification_numerator": numerator,
        "clarification_rate": numerator / len(eligible) if eligible else None,
        "excluded_interaction_count": len(interactions) - len(eligible),
        "active_technician_count": len(result["dim_technician"]),
        "topic_interaction_count": dict(
            Counter(f["topic"] for f in facts if f["topic"] is not None)
        ),
    }
    reconcile(result)
    return result


def reconcile(gold: dict) -> None:
    for table, key in [
        ("fact_conversation", "conversation_id"),
        ("fact_message", "message_id"),
        ("fact_interaction", "interaction_id"),
    ]:
        if len({r[key] for r in gold[table]}) != len(gold[table]):
            raise ValueError("GOLD_DUPLICATE_KEY")
    parents = {c["conversation_id"] for c in gold["fact_conversation"]}
    if any(
        r["conversation_id"] not in parents
        for t in ("fact_message", "fact_interaction")
        for r in gold[t]
    ):
        raise ValueError("GOLD_FOREIGN_KEY")
    if sum(c["message_count"] for c in gold["fact_conversation"]) != len(gold["fact_message"]):
        raise ValueError("GOLD_COUNT_MISMATCH")
