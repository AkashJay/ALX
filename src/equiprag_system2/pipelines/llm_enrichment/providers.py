"""No Spark UDFs: inference happens in bounded job workers."""

import json
import os
import re
from dataclasses import dataclass, field
from typing import Protocol
from urllib.parse import urlsplit


class ProviderError(Exception):
    def __init__(self, category: str, retryable: bool = False):
        super().__init__(category)
        self.category = category
        self.retryable = retryable


@dataclass
class Response:
    candidate: dict
    usage: dict = field(default_factory=dict)
    request_id: str | None = None


class Provider(Protocol):
    def generate(self, interaction: dict) -> Response: ...


def minimize(interaction: dict) -> dict:
    messages = []
    for m in interaction["messages"]:
        text = re.sub(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b", "[EMAIL]", m["content"])
        text = re.sub(r"(?i)\b(?:api[_ -]?key|password|secret)\s*[:=]\s*\S+", "[SECRET]", text)
        text = re.sub(r"\b(?:\+?\d[\d ()-]{8,}\d)\b", "[PHONE]", text)
        messages.append({"message_id": m["message_id"], "role": m["role"], "content": text})
    return {"interaction_id": interaction["interaction_id"], "messages": messages}


class MockProvider:
    """Explicit synthetic extractor, deliberately does not judge assistant safety."""

    def __init__(self):
        self.calls = 0

    def generate(self, interaction: dict) -> Response:
        self.calls += 1
        result: dict = {
            k: None
            for k in (
                "topic",
                "intent",
                "equipment",
                "component",
                "clarification_requested",
                "follow_up_present",
                "resolution_confirmed_by_conversation",
            )
        }
        result.update(reported_symptoms=[], candidate_guidance_issue="NONE", evidence=[])
        for message in interaction["messages"]:
            if message["role"] != "user":
                continue
            for claim, phrase in [("topic", "overheating"), ("equipment", "hydraulic pump")]:
                if phrase in message["content"]:
                    result[claim] = phrase
                    result["evidence"].append(
                        {"claim": claim, "message_id": message["message_id"], "excerpt": phrase}
                    )
        return Response(result)


class AzureProvider:
    token_parameter = "max_completion_tokens"

    def __init__(
        self,
        *,
        endpoint: str,
        deployment: str,
        api_version: str,
        schema: dict,
        prompt: str,
        taxonomy: dict,
        max_output_tokens: int,
        credential=None,
        client=None,
    ):
        if not endpoint.startswith("https://"):
            raise ValueError("HTTPS endpoint required")
        self.deployment, self.schema, self.prompt = deployment, schema, prompt
        self.taxonomy, self.max_output_tokens = taxonomy, max_output_tokens
        if client is None:
            from azure.identity import DefaultAzureCredential, get_bearer_token_provider
            from openai import AzureOpenAI

            token = get_bearer_token_provider(
                credential or DefaultAzureCredential(),
                "https://cognitiveservices.azure.com/.default",
            )
            client = AzureOpenAI(
                azure_endpoint=endpoint,
                api_version=api_version,
                azure_ad_token_provider=token,
                max_retries=0,
                timeout=60,
            )
        self.client = client

    def generate(self, interaction: dict) -> Response:
        try:
            result = self.client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {"role": "system", "content": self.prompt},
                    {
                        "role": "user",
                        "content": json.dumps({"taxonomy": self.taxonomy, **interaction}),
                    },
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "interaction_enrichment",
                        "strict": True,
                        "schema": self.schema,
                    },
                },
                **{self.token_parameter: self.max_output_tokens},
            )
            choice = result.choices[0]
            if getattr(choice.message, "refusal", None) or choice.finish_reason == "content_filter":
                raise ProviderError("REFUSED")
            if choice.finish_reason != "stop":
                raise ProviderError("INCOMPLETE_RESPONSE")
            content = choice.message.content
            if isinstance(content, list):
                content = "".join(block["text"] for block in content if block.get("type") == "text")
            candidate = json.loads(content)
            if not isinstance(candidate, dict):
                raise ProviderError("INVALID_RESPONSE")
            return Response(
                candidate,
                result.usage.model_dump() if result.usage else {},
                result.id,
            )
        except ProviderError:
            raise
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            retry = (
                status == 429
                or (isinstance(status, int) and status >= 500)
                or (type(exc).__name__ in ("APITimeoutError", "APIConnectionError", "TimeoutError"))
            )
            raise ProviderError(
                "TRANSIENT_PROVIDER_ERROR" if retry else "PROVIDER_ERROR", retryable=retry
            ) from None


def validate_databricks_endpoint(endpoint: str) -> str:
    """Validate before attaching a workspace credential; never accept arbitrary hosts."""
    uri = urlsplit(endpoint)
    host = uri.hostname or ""
    if (
        uri.scheme != "https"
        or not any(
            host.endswith(suffix)
            for suffix in (".cloud.databricks.com", ".azuredatabricks.net", ".gcp.databricks.com")
        )
        or uri.username is not None
        or uri.password is not None
        or uri.port not in (None, 443)
        or uri.query
        or uri.fragment
        or uri.path.rstrip("/") != "/ai-gateway/mlflow/v1"
        or any(c.isspace() for c in endpoint)
    ):
        raise ValueError("Configure an HTTPS Databricks /ai-gateway/mlflow/v1 endpoint")
    return endpoint.rstrip("/")


class DatabricksProvider(AzureProvider):
    """Dev hosted-model adapter using the same strict response and error handling."""

    token_parameter = "max_tokens"

    def __init__(
        self,
        *,
        endpoint: str,
        deployment: str,
        schema: dict,
        prompt: str,
        taxonomy: dict,
        max_output_tokens: int,
        api_key: str | None = None,
        client=None,
    ):
        endpoint = validate_databricks_endpoint(endpoint)
        if client is None:
            from openai import OpenAI

            token = api_key or os.environ.get("DATABRICKS_TOKEN")
            if not token:
                raise ValueError("Databricks runtime authentication is required")
            client = OpenAI(api_key=token, base_url=endpoint, max_retries=0, timeout=60)
        super().__init__(
            endpoint=endpoint,
            deployment=deployment,
            api_version="not-applicable",
            schema=schema,
            prompt=prompt,
            taxonomy=taxonomy,
            max_output_tokens=max_output_tokens,
            client=client,
        )
