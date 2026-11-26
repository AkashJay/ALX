"""Stable request identity inputs shared by local and cloud coordinators."""

from equiprag_system2.common.config_loader import EnrichmentSettings
from equiprag_system2.common.state import digest


def build_provenance(
    cfg: EnrichmentSettings,
    prompt: str,
    schema: dict,
    taxonomy: dict,
    *,
    endpoint: str = "",
    api_version: str = "",
) -> dict:
    value = {
        key: getattr(cfg, key)
        for key in (
            "prompt_version",
            "output_schema_version",
            "taxonomy_version",
            "model_deployment_revision",
            "provider",
            "model_deployment",
        )
    }
    value.update(
        endpoint_hash=digest(endpoint.rstrip("/")) if cfg.provider != "mock" else None,
        api_version=api_version if cfg.provider == "azure_openai" else None,
        prompt_hash=digest(prompt),
        schema_hash=digest(schema),
        taxonomy_hash=digest(taxonomy),
        transform_version="v1",
        validator_version="v1",
    )
    return value
