"""Command-line entry point for the incremental parser service."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from .producer import JsonlEventSink, KafkaEventSink, NullEventSink
from .service import ParserService
from .state import StateStore


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse Python files one at a time and emit CPG events"
    )
    parser.add_argument("--repo", default="accelerate", help="Source repository root")
    parser.add_argument(
        "--repository-name", default="huggingface/accelerate", help="Stable repository ID"
    )
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--file", help="Parse exactly one repository-relative .py file")
    selector.add_argument(
        "--manifest",
        default="output/file_discovery.json",
        help="File discovery JSON containing kept_files",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--state-dir", default="output/parser_state", help="Per-file replay state"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Write JSONL instead of connecting to Kafka"
    )
    parser.add_argument(
        "--discard-events",
        action="store_true",
        help="Parse and validate all files without retaining event payloads",
    )
    parser.add_argument(
        "--output-jsonl",
        default="output/parser_events.jsonl",
        help="Dry-run event output",
    )
    parser.add_argument(
        "--skip-unchanged", action="store_true", help="Skip files whose SHA-256 is unchanged"
    )
    parser.add_argument("--max-files", type=int, help="Limit manifest files for a smoke test")
    return parser


def _safe_source_path(repo_root: Path, relative: str) -> tuple[Path, str]:
    normalized = Path(relative.replace("\\", "/"))
    absolute = (repo_root / normalized).resolve()
    if absolute != repo_root and repo_root not in absolute.parents:
        raise ValueError(f"Đường dẫn nằm ngoài repo: {relative}")
    if absolute.suffix.lower() != ".py":
        raise ValueError(f"Không phải file Python: {relative}")
    return absolute, normalized.as_posix()


def _manifest_files(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    files = payload.get("kept_files")
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        raise ValueError("Manifest không có mảng kept_files hợp lệ")
    return files


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    repo_root = Path(args.repo).resolve()
    if not repo_root.is_dir():
        print(f"[ERROR] Không tìm thấy repo: {repo_root}", file=sys.stderr)
        return 2

    try:
        if args.file:
            relative_files = [args.file]
        else:
            relative_files = _manifest_files(Path(args.manifest))
        if args.max_files is not None:
            if args.max_files < 1:
                raise ValueError("--max-files phải >= 1")
            relative_files = relative_files[: args.max_files]
        source_files = [_safe_source_path(repo_root, item) for item in relative_files]
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 2

    if args.discard_events:
        sink = NullEventSink()
    elif args.dry_run:
        sink = JsonlEventSink(Path(args.output_jsonl))
    else:
        sink = KafkaEventSink(args.bootstrap_servers)
    service = ParserService(
        sink=sink,
        state_store=StateStore(Path(args.state_dir)),
        repository=args.repository_name,
        skip_unchanged=args.skip_unchanged,
    )
    outcomes = []
    try:
        for index, (absolute, relative) in enumerate(source_files, start=1):
            outcome = service.process_file(absolute, relative)
            outcomes.append(outcome)
            detail = f"nodes={outcome.nodes} edges={outcome.edges}"
            if outcome.error:
                detail = f"error={outcome.error}"
            print(
                f"[{index}/{len(source_files)}] {outcome.status.upper():9} {relative} {detail}",
                file=sys.stderr,
            )
    finally:
        sink.close()

    statuses = Counter(item.status for item in outcomes)
    summary = {
        "files_total": len(outcomes),
        "succeeded": statuses["succeeded"],
        "failed": statuses["failed"],
        "skipped": statuses["skipped"],
        "nodes": sum(item.nodes for item in outcomes),
        "edges": sum(item.edges for item in outcomes),
        "deleted_nodes": sum(item.deleted_nodes for item in outcomes),
        "deleted_edges": sum(item.deleted_edges for item in outcomes),
    }
    print(json.dumps(summary, indent=2), file=sys.stderr)
    return 1 if statuses["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
