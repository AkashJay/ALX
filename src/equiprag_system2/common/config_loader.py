"""Fail-closed configuration, precedence and identifier handling."""

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

PIPELINES = (
    "conversation_ingestion",
    "silver_processing",
    "interaction_processing",
    "llm_enrichment",
    "gold_processing",
)
SOURCES = ("conversations", "messages", "feedback")
ENV_KEYS = {"ADLS_LANDING_ROOT", "UC_CATALOG", "AZURE_OPENAI_DEPLOYMENT"}


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Schedule(Strict):
    timezone: Literal["UTC"] = "UTC"
    cron: str = "0 0 * * * ?"


class Processing(Strict):
    dry_run: bool = True
    max_records_per_batch: int = Field(default=1000, ge=1, le=100000)
    fail_on_unknown_source_schema: Literal[True] = True


class Storage(Strict):
    landing_root: str
    catalog: str
    schemas: dict[str, str]

    @field_validator("catalog")
    @classmethod
    def identifier(cls, value: str) -> str:
        quote_identifier(value)
        return value

    @field_validator("schemas")
    @classmethod
    def schema_names(cls, value: dict[str, str]) -> dict[str, str]:
        if set(value) != {"bronze", "silver", "enrichment", "gold", "ops"}:
            raise ValueError("invalid schema registry")
        for name in value.values():
            quote_identifier(name)
        return value


class Security(Strict):
    redact_text_before_llm: Literal[True] = True
    allow_raw_transcripts_in_gold: Literal[False] = False


class Settings(Strict):
    project: Literal["equiprag_system2"]
    environment: Literal["dev", "stg", "prod"]
    schedule: Schedule
    processing: Processing
    storage: Storage
    security: Security


class EnrichmentSettings(Strict):
    name: Literal["llm_enrichment"]
    layer: Literal["enrichment"]
    engine: Literal["serverless_python_job"]
    enabled: bool = True
    request_batch_size: int = Field(default=100, ge=1, le=10000)
    max_concurrency: int = Field(default=4, ge=1, le=32)
    max_attempts: int = Field(default=3, ge=1, le=10)
    prompt_version: str
    output_schema_version: str
    taxonomy_version: str
    provider: Literal["mock", "azure_openai"]
    model_deployment: str
    model_deployment_revision: str = "mock-v1"
    require_message_evidence: Literal[True]
    publish_only_validated: Literal[True]
    requests_per_minute: int = Field(default=30, ge=1)
    tokens_per_minute: int = Field(default=30000, ge=1)
    max_input_chars: int = Field(default=12000, ge=1)
    max_output_tokens: int = Field(default=1500, ge=1)
    request_overhead_chars: int = Field(default=0, ge=0)
    lease_seconds: int = Field(default=300, ge=120)


def quote_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("unsafe SQL identifier")
    return f"`{value}`"


def table_name(settings: Settings, layer: str, table: str) -> str:
    if layer not in settings.storage.schemas:
        raise ValueError("unknown layer")
    return ".".join(
        quote_identifier(x)
        for x in (settings.storage.catalog, settings.storage.schemas[layer], table)
    )


def merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for key, value in override.items():
        result[key] = (
            merge(result[key], value)
            if isinstance(value, dict) and isinstance(result.get(key), dict)
            else value
        )
    return result


def resolve(value: Any, env: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {k: resolve(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, env) for v in value]
    if isinstance(value, str) and "${" in value:
        match = re.fullmatch(r"\$\{([A-Z_]+)\}", value)
        if not match or match[1] not in ENV_KEYS or not env.get(match[1]):
            raise ValueError("missing or unapproved configuration reference")
        return env[match[1]]
    return value


def load(root: Path, environment: str, env: dict[str, str] | None = None) -> Settings:
    if environment not in ("dev", "stg", "prod"):
        raise ValueError("unknown environment")
    data = merge(
        yaml.safe_load((root / "config/defaults.yaml").read_text()),
        yaml.safe_load((root / f"config/environments/{environment}.yaml").read_text()),
    )
    return Settings.model_validate(resolve(data, dict(os.environ) if env is None else env))


def pipeline_config(root: Path, name: str, env: dict[str, str]) -> dict:
    if name not in PIPELINES:
        raise ValueError("unregistered pipeline")
    data = resolve(yaml.safe_load((root / f"pipelines/{name}/pipeline.yaml").read_text()), env)
    if set(data) != {"pipeline"} or data["pipeline"]["name"] != name:
        raise ValueError("invalid pipeline configuration")
    model = PIPELINE_MODELS[name]
    return model.model_validate(data["pipeline"]).model_dump()


class IngestionSettings(Strict):
    name: Literal["conversation_ingestion"]
    layer: Literal["bronze"]
    mode: Literal["incremental_files"]
    engine: Literal["lakeflow_sdp"]
    checkpoint_policy: Literal["managed_per_feed"]
    quarantine_on_invalid: Literal[True]


class SilverSettings(Strict):
    name: Literal["silver_processing"]
    layer: Literal["silver"]
    engine: Literal["lakeflow_sdp_or_serverless_job"]
    change_precedence: Literal["source_revision_then_timestamp"]
    on_ambiguous_revision: Literal["quarantine"]
    delete_semantics: Literal["tombstone"]


class InteractionSettings(Strict):
    name: Literal["interaction_processing"]
    layer: Literal["silver"]
    engine: Literal["serverless_python_job"]
    use_turn_id_when_trustworthy: Literal[True]
    ambiguous_policy: Literal["review_required"]
    invalidate_on_source_hash_change: Literal[True]


class GoldSettings(Strict):
    name: Literal["gold_processing"]
    layer: Literal["gold"]
    engine: Literal["serverless_pyspark_or_declarative"]
    publish_validated_only: Literal[True]
    exclude_raw_conversation_text: Literal[True]
    reconciliation_required: Literal[True]


PIPELINE_MODELS: dict[str, type[Strict]] = {
    "conversation_ingestion": IngestionSettings,
    "silver_processing": SilverSettings,
    "interaction_processing": InteractionSettings,
    "llm_enrichment": EnrichmentSettings,
    "gold_processing": GoldSettings,
}


class SourceSettings(Strict):
    name: Literal["conversations", "messages", "feedback"]
    type: Literal["adls_gen2"]
    format: Literal["jsonl"]
    relative_path: str
    schema_path: str = Field(alias="schema")
    target: str

    @field_validator("relative_path", "schema_path")
    @classmethod
    def safe_path(cls, value: str) -> str:
        if Path(value).is_absolute() or ".." in Path(value).parts:
            raise ValueError("unsafe source path")
        return value


def source_registry(root: Path) -> list[SourceSettings]:
    data = yaml.safe_load((root / "pipelines/conversation_ingestion/sources.yaml").read_text())
    rows = [SourceSettings.model_validate(row) for row in data["sources"]]
    if sorted(row.name for row in rows) != sorted(SOURCES):
        raise ValueError("source registry mismatch")
    for row in rows:
        if row.target != "bronze." + row.name + "_raw":
            raise ValueError("unapproved source target")
    return rows
