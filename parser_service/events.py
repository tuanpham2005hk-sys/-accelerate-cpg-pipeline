"""Kafka event helpers and topic contract for the parser service."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .identifiers import stable_hash

SCHEMA_VERSION = "1.0.0"

NODE_TOPIC = "cpg.node.events"
EDGE_TOPIC = "cpg.edge.events"
METADATA_TOPIC = "cpg.source.metadata.events"
ERROR_TOPIC = "cpg.parser.error.events"


def utc_event_time() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def base_event(event_type: str, event_time: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time": event_time or utc_event_time(),
        "event_type": event_type,
    }


def delete_node_event(
    node_identifier: str, file_path: str, event_time: str
) -> dict[str, Any]:
    return {
        **base_event("node_delete", event_time),
        "node_id": node_identifier,
        "file_path": file_path,
    }


def delete_edge_event(
    edge_identifier: str,
    file_path: str,
    event_time: str,
    src_node_id: str | None = None,
) -> dict[str, Any]:
    event = {
        **base_event("edge_delete", event_time),
        "edge_id": edge_identifier,
        "file_path": file_path,
    }
    if src_node_id is not None:
        event["src_node_id"] = src_node_id
    return event


def parser_error_event(
    *,
    repository: str,
    file_path: str,
    content_hash: str,
    stage: str,
    error: BaseException,
    event_time: str | None = None,
) -> dict[str, Any]:
    error_type = type(error).__name__
    message = str(error)
    stack_hash = stable_hash(error_type, message, stage, prefix="stack")
    return {
        **base_event("parse_error", event_time),
        "error_id": stable_hash(
            repository, file_path, content_hash, stage, error_type, message, prefix="err"
        ),
        "repository": repository,
        "file_path": file_path,
        "content_sha256": content_hash,
        "error_type": error_type,
        "error_message": message,
        "stage": stage,
        "severity": "error",
        "stack_hash": stack_hash,
    }
