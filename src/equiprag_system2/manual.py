"""Typed notebook parameters and ordered manual stage invocation."""

import re
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from equiprag_system2.cloud import TASKS, execute
from equiprag_system2.common.config_loader import Strict, load, quote_identifier
from equiprag_system2.pipelines.conversation_ingestion.manual import ingest

STAGES = ("bronze", *TASKS)


def validate_storage_path(value: str) -> str:
    value = value.rstrip("/")
    if any(c in value for c in ("?", "#", "%", "\\")) or any(c.isspace() for c in value):
        raise ValueError("Storage paths must not contain credentials, query strings or escapes")
    if any(part in (".", "..") for part in value.split("/")):
        raise ValueError("Storage path traversal is prohibited")
    if value.startswith("/Volumes/"):
        parts = value.split("/")
        if len(parts) < 5 or any(not part for part in parts[2:]):
            raise ValueError("Volume path requires catalog, schema and volume")
        for identifier in parts[2:5]:
            quote_identifier(identifier)
        return value
    uri = urlsplit(value)
    if uri.scheme != "abfss" or not re.fullmatch(
        r"[a-z0-9][a-z0-9-]*@[a-z0-9]+\.dfs\.core\.windows\.net", uri.netloc
    ):
        raise ValueError("Use a managed /Volumes/catalog/schema/volume path or ADLS Gen2 URI")
    return value


class ManualSettings(Strict):
    root: Path
    environment: Literal["dev"] = "dev"
    catalog: str
    landing_root: str
    checkpoint_root: str
    run_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    allow_writes: bool = False
    provider: Literal["mock", "azure_openai"] = "mock"
    deployment: str = "mock"
    deployment_revision: str = "mock-v1"
    endpoint: str = "https://unconfigured.invalid"
    api_version: str = "unconfigured"

    @field_validator("catalog")
    @classmethod
    def catalog_identifier(cls, value: str) -> str:
        quote_identifier(value)
        return value

    @field_validator("landing_root", "checkpoint_root")
    @classmethod
    def durable_path(cls, value: str) -> str:
        return validate_storage_path(value)

    @model_validator(mode="after")
    def validate_paths(self):
        if (
            self.checkpoint_root == self.landing_root
            or self.checkpoint_root.startswith(self.landing_root + "/")
            or self.landing_root.startswith(self.checkpoint_root + "/")
        ):
            raise ValueError("Checkpoint state must be outside the landing directory")
        if self.provider == "azure_openai" and (
            self.deployment == "mock"
            or self.deployment_revision == "mock-v1"
            or self.api_version == "unconfigured"
            or not self.endpoint.startswith("https://")
            or self.endpoint.endswith(".invalid")
        ):
            raise ValueError("Configure the approved live endpoint, API and deployment revision")
        return self


class ManualSession:
    """One active session per dev target; retry a failed stage before advancing."""

    def __init__(self, spark, config: ManualSettings):
        self.spark, self.config = spark, config
        self.completed: list[str] = []

    def run(self, stage: str) -> dict:
        cfg = self.config
        if not cfg.allow_writes:
            raise ValueError("Set allow_writes=true to opt into writes to the dev target")
        if stage not in STAGES:
            raise ValueError("Unknown stage")
        index = STAGES.index(stage)
        if self.completed != list(STAGES[:index]):
            raise ValueError("Run stages in order; create a new session for a complete replay")
        if stage == "bronze":
            settings = load(
                cfg.root,
                cfg.environment,
                {"UC_CATALOG": cfg.catalog, "ADLS_LANDING_ROOT": cfg.landing_root},
            )
            settings = settings.model_copy(
                update={"processing": settings.processing.model_copy(update={"dry_run": False})}
            )
            ingest(self.spark, settings, cfg.root, cfg.checkpoint_root)
        else:
            execute(
                self.spark,
                **cfg.model_dump(exclude={"checkpoint_root", "allow_writes"}),
                task=stage,
                manual_dev_writes=True,
            )
        self.completed.append(stage)
        return {"run_id": cfg.run_id, "stage": stage, "status": "SUCCEEDED"}


def widget_defaults() -> dict[str, str]:
    return {
        "root": "",
        "catalog": "",
        "landing_root": "",
        "checkpoint_root": "",
        "run_id": "",
        "allow_writes": "false",
        "provider": "mock",
        "deployment": "mock",
        "deployment_revision": "mock-v1",
        "endpoint": "https://unconfigured.invalid",
        "api_version": "unconfigured",
    }
