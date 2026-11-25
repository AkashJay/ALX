"""Small bounded Delta document adapter for restricted intermediate state.

Only registered tables are addressable. Writes are imperative job actions, never dataset
functions. The job has one orchestrator; optimistic fencing protects claimed work.
"""

import json
import uuid
from typing import Any

from equiprag_system2.common.config_loader import Settings, table_name
from equiprag_system2.common.state import dumps

TABLES = {
    "receipts": "silver",
    "conversation_state": "silver",
    "episode_state": "silver",
    "requests": "enrichment",
    "cursors": "ops",
    "worksets": "ops",
    "runs": "ops",
    "budgets": "ops",
    "publications": "gold",
}


class DeltaStore:
    def __init__(self, spark: Any, settings: Settings):
        self.spark, self.settings = spark, settings
        self.limit = settings.processing.max_records_per_batch

    def name(self, table: str) -> str:
        if table not in TABLES:
            raise ValueError("unregistered Delta state table")
        return table_name(self.settings, TABLES[table], table)

    def initialize(self) -> None:
        for table in TABLES:
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {self.name(table)} "
                "(key STRING NOT NULL, scope STRING, payload STRING NOT NULL, token STRING) "
                "USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
            )

    def rows(
        self, table: str, *, keys: list[str] | None = None, scopes: list[str] | None = None
    ) -> list[dict]:
        from pyspark.sql import functions as F

        frame = self.spark.table(self.name(table))
        if keys is not None:
            frame = frame.where(F.col("key").isin(keys))
        if scopes is not None:
            frame = frame.where(F.col("scope").isin(scopes))
        rows = frame.limit(self.limit + 1).collect()
        if len(rows) > self.limit:
            raise ValueError("BOUNDED_BATCH_EXCEEDED: narrow replay scope or approve larger limit")
        return [
            {"key": r.key, "scope": r.scope, "payload": json.loads(r.payload), "token": r.token}
            for r in rows
        ]

    def unfinished_worksets(self) -> list[dict]:
        from pyspark.sql import functions as F

        frame = self.spark.table(self.name("worksets")).where(
            F.coalesce(F.get_json_object("payload", "$.completed"), F.lit("false")) != "true"
        )
        rows = frame.limit(self.limit + 1).collect()
        if len(rows) > self.limit:
            raise ValueError("WORKSET_RECOVERY_LIMIT_EXCEEDED")
        return [{"key": row.key, "payload": json.loads(row.payload)} for row in rows]

    def get(self, table: str, key: str, default=None):
        rows = self.rows(table, keys=[key])
        return rows[0]["payload"] if rows else default

    def put(self, table: str, key: str, value: dict, scope: str = "") -> None:
        frame = self.spark.createDataFrame(
            [(key, scope, dumps(value), str(uuid.uuid4()))],
            "key string, scope string, payload string, token string",
        )
        view = "_equiprag_" + uuid.uuid4().hex
        frame.createOrReplaceTempView(view)
        try:
            self.spark.sql(
                f"MERGE INTO {self.name(table)} t USING {view} s ON t.key=s.key "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
        finally:
            self.spark.catalog.dropTempView(view)

    def compare_swap(self, table: str, key: str, expected: str, value: dict) -> bool:
        token = str(uuid.uuid4())
        # Spark SQL named parameters bind values; only allowlisted identifiers are interpolated.
        self.spark.sql(
            f"UPDATE {self.name(table)} SET payload=:payload,token=:token "
            "WHERE key=:key AND token=:expected",
            args={"payload": dumps(value), "token": token, "key": key, "expected": expected},
        )
        rows = self.rows(table, keys=[key])
        return bool(rows and rows[0]["token"] == token)
