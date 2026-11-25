"""Delta request identity and conditional fenced leases for bounded serverless tasks."""

import time
import uuid

from equiprag_system2.common.delta import DeltaStore
from equiprag_system2.common.state import digest


class DeltaLedger:
    def __init__(self, store: DeltaStore):
        self.store = store

    def reconcile(self, scope: str, interactions: list[dict], provenance: dict) -> None:
        desired = {
            digest([i["interaction_id"], i["source_content_hash"], provenance]): i
            for i in interactions
        }
        existing = {r["key"]: r for r in self.store.rows("requests", scopes=[scope])}
        for key, row in existing.items():
            value = row["payload"]
            if key not in desired and value["status"] != "SUPERSEDED":
                self._transition(value, "SUPERSEDED")
                self.store.compare_swap("requests", key, row["token"], value)
        for key, interaction in desired.items():
            if key not in existing:
                value = {
                    "key": key,
                    "input": interaction,
                    "provenance": provenance,
                    "status": "PENDING",
                    "attempts": 0,
                    "history": [],
                    "next_attempt": 0,
                    "expires": 0,
                    "candidate": None,
                    "validation": None,
                }
                self._transition(value, "PENDING")
                # Only the serialized create-requests task inserts new identities.
                self.store.put("requests", key, value, scope)
            elif existing[key]["payload"]["status"] == "SUPERSEDED":
                row = existing[key]
                value = row["payload"]
                status = (
                    "VALIDATED" if value["candidate"] and value["validation"] == [] else "PENDING"
                )
                self._transition(value, status)
                self.store.compare_swap("requests", key, row["token"], value)

    @staticmethod
    def _transition(value: dict, status: str, reason: str | None = None) -> None:
        value["status"] = status
        value["history"].append({"status": status, "at": time.time(), "reason": reason})

    def claim(self, row: dict, max_attempts: int, lease_seconds: int) -> dict | None:
        now, value = time.time(), row["payload"]
        if value["status"] == "PROCESSING" and value["expires"] > now:
            return None
        if value["status"] not in ("PENDING", "RETRYABLE_FAILED", "PROCESSING"):
            return None
        if value["attempts"] >= max_attempts:
            self._transition(value, "PERMANENT_FAILED", "ATTEMPTS_EXHAUSTED")
            self.store.compare_swap("requests", row["key"], row["token"], value)
            return None
        if value["next_attempt"] > now:
            return None
        value.update(
            attempts=value["attempts"] + 1, expires=now + lease_seconds, lease=str(uuid.uuid4())
        )
        self._transition(value, "PROCESSING")
        if not self.store.compare_swap("requests", row["key"], row["token"], value):
            return None
        return self.store.rows("requests", keys=[row["key"]])[0]

    def finish(
        self, row: dict, status: str, *, candidate=None, errors=None, error=None, delay: float = 0
    ) -> bool:
        value = row["payload"]
        if value["expires"] <= time.time():
            return False
        value.update(
            candidate=candidate, validation=errors, error=error, next_attempt=time.time() + delay
        )
        self._transition(value, status, error)
        return self.store.compare_swap("requests", row["key"], row["token"], value)
