"""Deterministic conservative episodes; no model-defined boundaries."""

from collections import defaultdict

from equiprag_system2.common.state import digest
from equiprag_system2.contracts.events import utc_timestamp


def _content(event: dict) -> dict:
    return {
        k: v for k, v in event.items() if k not in ("exported_at", "export_batch_id", "event_id")
    }


def build(silver: dict, previous: list[dict]) -> dict:
    by_conversation: dict = defaultdict(list)
    for message in silver["active"]["messages"]:
        by_conversation[message["conversation_id"]].append(message)
    interactions: list[dict] = []
    bridge: list[dict] = []
    rejected: list[dict] = []
    conversations = {c["conversation_id"]: c for c in silver["active"]["conversations"]}
    for cid, messages in sorted(by_conversation.items()):
        messages.sort(key=lambda m: (m["sequence_no"], m["message_id"]))
        if len({m["sequence_no"] for m in messages}) != len(messages) or any(
            utc_timestamp(a["created_at"]) > utc_timestamp(b["created_at"])
            for a, b in zip(messages, messages[1:])
        ):
            rejected.append({"conversation_id": cid, "reason": "AMBIGUOUS_MESSAGE_ORDER"})
            continue
        groups: dict = defaultdict(list)
        if all(m.get("turn_id") for m in messages):
            seen, last = set(), None
            invalid = False
            for m in messages:
                turn = m["turn_id"]
                if turn != last and turn in seen:
                    invalid = True
                seen.add(turn)
                last = turn
                groups[turn].append(m)
            if invalid:
                rejected.append({"conversation_id": cid, "reason": "NONCONTIGUOUS_TURN"})
                continue
        elif any(m.get("turn_id") for m in messages):
            rejected.append({"conversation_id": cid, "reason": "MIXED_TURN_IDS"})
            continue
        else:
            # Only unambiguous alternating pairs are accepted without a reliable source turn.
            if len(messages) % 2 or any(
                m["role"] != ("user" if i % 2 == 0 else "assistant") for i, m in enumerate(messages)
            ):
                rejected.append({"conversation_id": cid, "reason": "AMBIGUOUS_SEGMENTATION"})
                continue
            for i in range(0, len(messages), 2):
                groups["fallback:" + messages[i]["message_id"]] = messages[i : i + 2]
        for turn, members in sorted(groups.items()):
            roles = [m["role"] for m in members]
            if roles != ["user", "assistant"]:
                rejected.append(
                    {"conversation_id": cid, "turn_id": turn, "reason": "UNAPPROVED_COMPLEX_TURN"}
                )
                continue
            iid = digest([cid, turn])
            version = digest(
                {
                    "messages": [_content(m) for m in members],
                    "conversation": _content(conversations[cid]),
                }
            )
            interactions.append(
                {
                    "interaction_id": iid,
                    "conversation_id": cid,
                    "source_content_hash": version,
                    "messages": members,
                    "created_at": members[0]["created_at"],
                }
            )
            bridge.extend(
                {"interaction_id": iid, "message_id": m["message_id"], "ordinal": i}
                for i, m in enumerate(members)
            )
    old = {i["interaction_id"]: i["source_content_hash"] for i in previous}
    new = {i["interaction_id"]: i["source_content_hash"] for i in interactions}
    changed = sorted(k for k in old.keys() | new.keys() if old.get(k) != new.get(k))
    return {
        "interactions": interactions,
        "bridge": bridge,
        "changed": changed,
        "rejected": rejected,
    }
