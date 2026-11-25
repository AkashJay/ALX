"""Shared pure Auto Loader receipt reader."""

from typing import Any

from equiprag_system2.common.config_loader import Settings


def read_feed(spark: Any, settings: Settings, source: str, relative_path: str):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField("value", StringType(), True)])
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.allowOverwrites", "false")
        .schema(schema)
        .load(settings.storage.landing_root.rstrip("/") + "/" + relative_path)
        .select(
            F.lit(source).alias("source"),
            F.col("value").alias("raw"),
            F.col("_metadata.file_path").alias("source_filename"),
            F.col("_metadata.file_modification_time").alias("file_modified_at"),
            F.current_timestamp().alias("ingested_at"),
        )
        .withColumn(
            "receipt_id",
            F.sha2(F.concat_ws("\u001f", "source", "source_filename", "raw"), 256),
        )
    )
