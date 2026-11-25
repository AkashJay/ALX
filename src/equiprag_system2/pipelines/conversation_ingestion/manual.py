"""Imperative, finite Bronze ingestion for manual dev validation."""

from pathlib import Path

from equiprag_system2.common.config_loader import Settings, source_registry, table_name
from equiprag_system2.pipelines.conversation_ingestion.reader import read_feed


def ingest(spark, settings: Settings, root: Path, checkpoint_root: str) -> None:
    if settings.processing.dry_run:
        raise ValueError("Manual writes are disabled")
    for entry in source_registry(root):
        target = table_name(settings, "bronze", entry.name + "_raw")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target} (source STRING,raw STRING,"
            "source_filename STRING,file_modified_at TIMESTAMP,ingested_at TIMESTAMP,"
            "receipt_id STRING) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
        )
        query = (
            read_feed(spark, settings, entry.name, entry.relative_path)
            .writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_root.rstrip("/") + "/" + entry.name)
            .trigger(availableNow=True)
            .toTable(target)
        )
        query.awaitTermination()
