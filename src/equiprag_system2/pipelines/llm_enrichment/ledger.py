"""Atomic local lease claims with fencing, retries and supersession history."""

import json
import time
import uuid

from equiprag_system2.common.state import State, digest, dumps


class Ledger:
    def __init__(self, state: State):
        self.state = state

    def transition(self, key: str, status: str, now: float, reason: str | None = None) -> None:
        self.state.db.execute(
            "INSERT INTO transitions(key,status,at,reason) VALUES (?,?,?,?)",
            (key, status, now, reason),
        )

    def reconcile(self, interactions: list[dict], provenance: dict, now: float | None = None):
        now = time.time() if now is None else now
        desired = {}
        for interaction in interactions:
            key = digest(
                [interaction["interaction_id"], interaction["source_content_hash"], provenance]
            )
            desired[key] = interaction
        with self.state.db:
            for row in self.state.db.execute("SELECT key,status FROM requests").fetchall():
                if row["key"] not in desired and row["status"] != "SUPERSEDED":
                    self.state.db.execute(
                        "UPDATE requests SET status='SUPERSEDED',lease=NULL,updated=? WHERE key=?",
                        (now, row["key"]),
                    )
                    self.transition(row["key"], "SUPERSEDED", now)
            for key, interaction in desired.items():
                cur = self.state.db.execute(
                    "INSERT OR IGNORE INTO requests "
                    "(key,interaction_id,content_hash,status,input,provenance,created,updated) "
                    "VALUES (?,?,?,'PENDING',?,?,?,?)",
                    (
                        key,
                        interaction["interaction_id"],
                        interaction["source_content_hash"],
                        dumps(interaction),
                        dumps(provenance),
                        now,
                        now,
                    ),
                )
                if cur.rowcount:
                    self.transition(key, "PENDING", now)
                # Reactivating the exact earlier identity reuses its validated candidate.
                self.state.db.execute(
                    "UPDATE requests SET status=CASE WHEN validation='[]' "
                    "AND candidate IS NOT NULL THEN 'VALIDATED' ELSE 'PENDING' END,updated=? "
                    "WHERE key=? AND status='SUPERSEDED'",
                    (now, key),
                )

    def claim(self, max_attempts: int, lease_seconds: int, now: float | None = None) -> dict | None:
        now = time.time() if now is None else now
        db = self.state.db
        db.execute("BEGIN IMMEDIATE")
        try:
            expired = db.execute(
                "SELECT key FROM requests WHERE status='PROCESSING' AND expires<=? AND attempts>=?",
                (now, max_attempts),
            ).fetchall()
            for row in expired:
                db.execute(
                    "UPDATE requests SET status='PERMANENT_FAILED',error='LEASE_EXHAUSTED' "
                    "WHERE key=?",
                    (row["key"],),
                )
                self.transition(row["key"], "PERMANENT_FAILED", now, "LEASE_EXHAUSTED")
            row = db.execute(
                "SELECT * FROM requests WHERE attempts<? AND "
                "((status IN ('PENDING','RETRYABLE_FAILED') AND next_attempt<=?) OR "
                "(status='PROCESSING' AND expires<=?)) ORDER BY created,key LIMIT 1",
                (max_attempts, now, now),
            ).fetchone()
            if row is None:
                db.commit()
                return None
            token = str(uuid.uuid4())
            db.execute(
                "UPDATE requests SET status='PROCESSING',attempts=attempts+1,lease=?,"
                "expires=?,updated=? WHERE key=?",
                (token, now + lease_seconds, now, row["key"]),
            )
            self.transition(row["key"], "PROCESSING", now)
            db.commit()
            return {
                **dict(row),
                "lease": token,
                "attempts": row["attempts"] + 1,
                "input": json.loads(row["input"]),
            }
        except BaseException:
            db.rollback()
            raise

    def finish(
        self,
        claim: dict,
        status: str,
        *,
        candidate: dict | None = None,
        errors: list | None = None,
        error: str | None = None,
        delay: float = 0,
        now: float | None = None,
    ) -> bool:
        now = time.time() if now is None else now
        with self.state.db:
            changed = self.state.db.execute(
                "UPDATE requests SET status=?,candidate=?,validation=?,"
                "error=?,next_attempt=?,lease=NULL,updated=? WHERE key=? AND lease=? "
                "AND status='PROCESSING' AND expires>?",
                (
                    status,
                    dumps(candidate) if candidate is not None else None,
                    dumps(errors or []),
                    error,
                    now + delay,
                    now,
                    claim["key"],
                    claim["lease"],
                    now,
                ),
            ).rowcount
            if changed:
                if candidate is not None:
                    self.state.put(
                        "candidate_attempt:" + claim["key"] + ":" + str(claim["attempts"]),
                        {"candidate": candidate, "validation": errors or [], "status": status},
                    )
                self.transition(claim["key"], status, now, error)
            return bool(changed)
