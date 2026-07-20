"""Deterministic identifiers used by the CPG event stream."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def stable_hash(*parts: Any, prefix: str) -> str:
    """Return a readable, deterministic SHA-256 identifier.

    JSON length-prefixing avoids ambiguous concatenation (for example, ``ab,c``
    versus ``a,bc``). Event timestamps are deliberately never passed here.
    """

    payload = json.dumps(parts, ensure_ascii=False, separators=(",", ":"), default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


def node_id(
    repository: str,
    file_path: str,
    scope: str,
    node_type: str,
    signature: str,
    occurrence: int,
) -> str:
    return stable_hash(
        repository,
        file_path,
        scope,
        node_type,
        signature,
        occurrence,
        prefix="n",
    )


def external_symbol_id(repository: str, file_path: str, symbol: str) -> str:
    # External symbols are file-owned so incremental cleanup of one file cannot
    # accidentally delete a symbol that was emitted by another source file.
    return stable_hash(repository, file_path, "external-symbol", symbol, prefix="n")


def edge_id(
    edge_type: str,
    src_node_id: str,
    dst_node_id: str,
    anchor: str = "",
) -> str:
    return stable_hash(edge_type, src_node_id, dst_node_id, anchor, prefix="e")


def file_id(repository: str, file_path: str) -> str:
    return stable_hash(repository, file_path, prefix="f")


def content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
