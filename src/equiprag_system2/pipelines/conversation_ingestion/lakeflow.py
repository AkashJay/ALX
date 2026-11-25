"""Pure Auto Loader definitions: one streaming dataset/checkpoint per approved feed."""

from pathlib import Path
from typing import Any

from equiprag_system2.common.config_loader import Settings, source_registry, table_name
from equiprag_system2.pipelines.conversation_ingestion.reader import read_feed


def register(spark: Any, settings: Settings, root: Path) -> None:
    from pyspark import pipelines as dp

    if settings.processing.dry_run:
        raise ValueError("Dry-run environment: ingestion writes disabled")
    if not settings.storage.landing_root.startswith("abfss://"):
        raise ValueError("Cloud ingestion requires an approved ADLS Gen2 URI")
    # Preserve each original JSONL line, including malformed JSON and unknown properties.
    # Contract parsing/quarantine is done by the downstream bounded job. Text schema is
    # fixed; original payload is the rescue source, so unknown data cannot disappear.
    for entry in source_registry(root):
        source = entry.name
        relative_path = entry.relative_path

        def definition(source=source, relative_path=relative_path):
            return read_feed(spark, settings, source, relative_path)

        dp.table(
            name=table_name(settings, "bronze", source + "_raw"),
            comment="Restricted immutable JSONL receipts; contract parsing in Silver job",
            table_properties={"delta.enableChangeDataFeed": "true"},
        )(definition)
