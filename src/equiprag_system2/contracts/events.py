"""Synthetic v1 event validation shared by local and cloud processing."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

KEYS = {"conversations": "conversation_id", "messages": "message_id", "feedback": "feedback_id"}
KNOWN = set(
    [
        "record_id",
        "source_updated_at",
        "operation",
        "schema_version",
        "exported_at",
        "source_revision",
        "event_id",
        "export_batch_id",
        "source_application_version",
        "conversation_id",
        "technician_id",
        "started_at",
        "ended_at",
        "channel",
        "status",
        "message_id",
        "role",
        "content",
        "created_at",
        "sequence_no",
        "turn_id",
        "source_doc_refs",
        "attachment_refs",
        "content_permission",
        "feedback_id",
        "rating",
        "comment",
    ]
)


class Envelope(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)
    record_id: str = Field(min_length=1)
    source_revision: int = Field(ge=1)
    operation: Literal["UPSERT", "DELETE"]
    schema_version: Literal[1, "1"]
    source_updated_at: str
    exported_at: str

    @field_validator("source_updated_at", "exported_at")
    @classmethod
    def timestamp(cls, value: str) -> str:
        utc_timestamp(value)
        return value


def utc_timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    offset = result.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("UTC timestamp required")
    return result


def validate_event(source: str, value: Any) -> tuple[dict | None, dict, str | None]:
    try:
        if source not in KEYS or not isinstance(value, dict):
            raise ValueError("invalid source/object")
        Envelope.model_validate(value)
        if isinstance(value["schema_version"], bool):
            raise ValueError("invalid schema version")
        for field in (
            "conversation_id",
            "message_id",
            "feedback_id",
            "event_id",
            "technician_id",
            "turn_id",
        ):
            if value.get(field) is not None and (
                not isinstance(value[field], str) or not value[field]
            ):
                raise ValueError("invalid identifier")
        key = KEYS[source]
        if not isinstance(value.get(key), str) or not value[key]:
            raise ValueError("missing entity key")
        if value["operation"] == "UPSERT":
            required = {
                "conversations": ("started_at", "channel"),
                "messages": ("conversation_id", "role", "created_at", "sequence_no"),
                "feedback": ("conversation_id", "created_at"),
            }[source]
            if any(value.get(k) is None for k in required):
                raise ValueError("missing required attribute")
            for name in ("started_at", "ended_at", "created_at"):
                if value.get(name) is not None:
                    utc_timestamp(value[name])
            if value.get("ended_at") and utc_timestamp(value["ended_at"]) < utc_timestamp(
                value["started_at"]
            ):
                raise ValueError("negative conversation duration")
            if source == "conversations" and value["channel"] not in ("text", "voice"):
                raise ValueError("invalid channel")
            if source == "messages":
                if value["role"] not in ("user", "assistant", "system", "tool"):
                    raise ValueError("invalid role")
                if type(value["sequence_no"]) is not int or value["sequence_no"] < 0:
                    raise ValueError("invalid sequence")
                if value.get("content_permission") != "approved":
                    return None, {}, "CONTENT_NOT_PERMITTED"
                if not isinstance(value.get("content"), str):
                    raise ValueError("invalid content")
        return (
            {k: v for k, v in value.items() if k in KNOWN},
            {k: v for k, v in value.items() if k not in KNOWN},
            None,
        )
    except (ValidationError, ValueError, TypeError, AttributeError, KeyError):
        return None, {}, "INVALID_SOURCE_CONTRACT"
