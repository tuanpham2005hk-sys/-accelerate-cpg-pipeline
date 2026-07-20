"""Incremental orchestration: parse, diff, publish, then commit state."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .cpg_parser import CPGParser, ParseResult
from .events import (
    EDGE_TOPIC,
    ERROR_TOPIC,
    METADATA_TOPIC,
    NODE_TOPIC,
    delete_edge_event,
    delete_node_event,
    parser_error_event,
    utc_event_time,
)
from .identifiers import content_sha256
from .producer import EventSink
from .state import FileState, StateStore


@dataclass(slots=True)
class ProcessOutcome:
    file_path: str
    status: str
    nodes: int = 0
    edges: int = 0
    deleted_nodes: int = 0
    deleted_edges: int = 0
    error: str | None = None


class ParserService:
    def __init__(
        self,
        *,
        sink: EventSink,
        state_store: StateStore,
        parser: CPGParser | None = None,
        repository: str = "huggingface/accelerate",
        skip_unchanged: bool = False,
    ) -> None:
        self.sink = sink
        self.state_store = state_store
        self.parser = parser or CPGParser(repository)
        self.repository = repository
        self.skip_unchanged = skip_unchanged

    def process_file(self, absolute_path: Path, file_path: str) -> ProcessOutcome:
        previous = self.state_store.load(file_path)
        raw = b""
        try:
            raw = absolute_path.read_bytes()
            digest = content_sha256(raw)
            if (
                self.skip_unchanged
                and previous is not None
                and previous.content_sha256 == digest
            ):
                return ProcessOutcome(file_path=file_path, status="skipped")
            result = self.parser.parse_file(absolute_path, file_path)
            return self._publish_result(result, previous)
        except Exception as error:  # one bad file must not stop the batch
            digest = content_sha256(raw) if raw else ""
            event = parser_error_event(
                repository=self.repository,
                file_path=file_path,
                content_hash=digest,
                stage=self._error_stage(error),
                error=error,
                event_time=utc_event_time(),
            )
            try:
                self.sink.publish(ERROR_TOPIC, file_path, event)
                self.sink.flush()
            except Exception as publish_error:
                return ProcessOutcome(
                    file_path=file_path,
                    status="failed",
                    error=f"{error}; additionally failed to publish error: {publish_error}",
                )
            return ProcessOutcome(file_path=file_path, status="failed", error=str(error))

    def _publish_result(
        self, result: ParseResult, previous: FileState | None
    ) -> ProcessOutcome:
        old_nodes = set(previous.node_ids) if previous else set()
        old_edges = set(previous.edge_sources) if previous else set()
        current_nodes = result.node_ids
        current_edge_sources = {
            edge["edge_id"]: edge["src_node_id"] for edge in result.edges
        }
        current_edges = set(current_edge_sources)
        removed_edges = sorted(old_edges - current_edges)
        removed_nodes = sorted(old_nodes - current_nodes)
        event_time = result.metadata["event_time"]

        # Relationships must disappear before their endpoint nodes.
        for identifier in removed_edges:
            source = previous.edge_sources.get(identifier) if previous else None
            event = delete_edge_event(
                identifier, result.file_path, event_time, src_node_id=source
            )
            self.sink.publish(EDGE_TOPIC, source or identifier, event)
        for identifier in removed_nodes:
            self.sink.publish(
                NODE_TOPIC,
                result.file_path,
                delete_node_event(identifier, result.file_path, event_time),
            )
        for event in result.nodes:
            self.sink.publish(NODE_TOPIC, result.file_path, event)
        for event in result.edges:
            self.sink.publish(EDGE_TOPIC, event["src_node_id"], event)
        self.sink.publish(METADATA_TOPIC, result.file_path, result.metadata)
        self.sink.flush()

        # Commit only after every Kafka future (or dry-run write) succeeded.
        self.state_store.save(
            result.file_path,
            FileState(
                content_sha256=result.content_hash,
                node_ids=sorted(current_nodes),
                edge_sources=dict(sorted(current_edge_sources.items())),
            ),
        )
        return ProcessOutcome(
            file_path=result.file_path,
            status="succeeded",
            nodes=len(result.nodes),
            edges=len(result.edges),
            deleted_nodes=len(removed_nodes),
            deleted_edges=len(removed_edges),
        )

    @staticmethod
    def _error_stage(error: Exception) -> str:
        if isinstance(error, SyntaxError):
            return "ast_parse"
        if isinstance(error, UnicodeError):
            return "decode"
        if isinstance(error, OSError):
            return "read_source"
        return "parser_service"
