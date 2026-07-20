"""Per-file parser state used to calculate incremental delete events."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from .identifiers import stable_hash


@dataclass(slots=True)
class FileState:
    content_sha256: str
    node_ids: list[str]
    edge_sources: dict[str, str]


class StateStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, file_path: str) -> Path:
        name = stable_hash(file_path, prefix="state").replace(":", "-")
        return self.root / f"{name}.json"

    def load(self, file_path: str) -> FileState | None:
        path = self._path(file_path)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        return FileState(
            content_sha256=payload["content_sha256"],
            node_ids=list(payload.get("node_ids", [])),
            edge_sources=dict(payload.get("edge_sources", {})),
        )

    def save(self, file_path: str, state: FileState) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        target = self._path(file_path)
        temporary = target.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(asdict(state), indent=2, sort_keys=True), encoding="utf-8"
        )
        os.replace(temporary, target)
