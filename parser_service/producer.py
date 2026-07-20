"""Event sink implementations for Kafka, JSONL dry runs, and tests."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Protocol, TextIO


class EventSink(Protocol):
    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class JsonlEventSink:
    """Write the Kafka envelope to JSONL without requiring a running broker."""

    def __init__(self, path: Path | None = None) -> None:
        self._owns_stream = path is not None
        if path is None:
            self.stream: TextIO = sys.stdout
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.stream = path.open("w", encoding="utf-8")

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        envelope = {"topic": topic, "key": key, "value": event}
        self.stream.write(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")))
        self.stream.write("\n")

    def flush(self) -> None:
        self.stream.flush()

    def close(self) -> None:
        if self._owns_stream:
            self.stream.close()


class KafkaEventSink:
    """Publish JSON events using kafka-python (imported only when requested)."""

    def __init__(self, bootstrap_servers: str) -> None:
        try:
            from kafka import KafkaProducer
        except ImportError as error:  # pragma: no cover - depends on local setup
            raise RuntimeError(
                "Thiếu kafka-python. Chạy: python -m pip install -r requirements.txt"
            ) from error

        self.producer = KafkaProducer(
            bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",")],
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=10,
        )
        self.pending = []

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        self.pending.append(self.producer.send(topic, key=key, value=event))

    def flush(self) -> None:
        self.producer.flush(timeout=30)
        pending, self.pending = self.pending, []
        for future in pending:
            future.get(timeout=30)

    def close(self) -> None:
        self.producer.close(timeout=30)


class MemoryEventSink:
    """Small in-memory sink used only by automated tests."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, Any]]] = []

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        self.events.append((topic, key, event))

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class NullEventSink:
    """Validate the full parser flow while discarding generated event payloads."""

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        return None

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None
