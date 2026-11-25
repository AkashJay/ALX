"""Transactional local adapter. Payload tables are restricted, never BI outputs."""

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


def dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: Any) -> str:
    return hashlib.sha256(dumps(value).encode()).hexdigest()


class State:
    def __init__(self, path: str | Path):
        self.db = sqlite3.connect(str(path), timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS files(source TEXT, uri TEXT, hash TEXT,
          PRIMARY KEY(source,uri));
        CREATE TABLE IF NOT EXISTS bronze(id TEXT PRIMARY KEY, source TEXT, uri TEXT,
          line INTEGER, ingested_at TEXT, raw TEXT, payload TEXT, rescued TEXT, reason TEXT);
        CREATE TABLE IF NOT EXISTS snapshots(name TEXT PRIMARY KEY, payload TEXT);
        CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY, interaction_id TEXT,
          content_hash TEXT, status TEXT, attempts INTEGER DEFAULT 0, lease TEXT,
          expires REAL, next_attempt REAL DEFAULT 0, input TEXT, candidate TEXT,
          validation TEXT, error TEXT, provenance TEXT, created REAL, updated REAL);
        CREATE TABLE IF NOT EXISTS transitions(id INTEGER PRIMARY KEY, key TEXT,
          status TEXT, at REAL, reason TEXT);
        CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY, payload TEXT);
        """)

    def read(self, name: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT payload FROM snapshots WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, name: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO snapshots VALUES (?,?) ON CONFLICT(name) DO UPDATE "
            "SET payload=excluded.payload",
            (name, dumps(value)),
        )

    def close(self) -> None:
        self.db.close()
