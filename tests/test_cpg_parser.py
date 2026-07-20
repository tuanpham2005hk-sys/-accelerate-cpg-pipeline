from __future__ import annotations

import json
from pathlib import Path

import pytest

from parser_service.__main__ import main
from parser_service.cpg_parser import CPGParser
from parser_service.events import ERROR_TOPIC, SCHEMA_VERSION
from parser_service.identifiers import edge_id, node_id
from parser_service.producer import MemoryEventSink
from parser_service.service import ParserService
from parser_service.state import StateStore


SAMPLE = """\
def helper(value):
    return value + 1

class Demo:
    def run(self, amount):
        total = helper(amount)
        if total > 0:
            result = total
        else:
            result = 0
        for item in range(2):
            result += item
        print(result)
        return result
"""


def write_source(root: Path, relative: str = "sample.py", content: str = SAMPLE) -> Path:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target


def parse(tmp_path: Path, content: str = SAMPLE):
    source = write_source(tmp_path, content=content)
    return CPGParser("example/repo").parse_file(source, "sample.py")


def test_identifier_helpers_are_stable() -> None:
    first_node = node_id("repo", "a.py", "scope", "Name", "x:Load", 0)
    second_node = node_id("repo", "a.py", "scope", "Name", "x:Load", 0)
    assert first_node == second_node
    assert first_node.startswith("n:")
    assert edge_id("DFG", first_node, "n:target", "x") == edge_id(
        "DFG", first_node, "n:target", "x"
    )


def test_parser_extracts_all_cpg_categories(tmp_path: Path) -> None:
    result = parse(tmp_path)
    edge_types = {event["edge_type"] for event in result.edges}
    assert {"AST_CHILD", "CFG", "DFG", "CALL"} <= edge_types
    assert result.metadata["counts"]["nodes"] == len(result.nodes)
    assert result.metadata["counts"]["edges"] == len(result.edges)
    assert result.metadata["counts"]["cfg_edges"] > 0
    assert result.metadata["counts"]["dfg_edges"] > 0
    assert result.metadata["counts"]["call_edges"] >= 3


def test_internal_and_external_calls_are_distinguished(tmp_path: Path) -> None:
    result = parse(tmp_path)
    nodes = {event["node_id"]: event for event in result.nodes}
    calls = [event for event in result.edges if event["edge_type"] == "CALL"]
    helper_call = next(event for event in calls if event["properties"]["target"] == "helper")
    print_call = next(event for event in calls if event["properties"]["target"] == "print")
    assert nodes[helper_call["dst_node_id"]]["node_type"] == "FunctionDef"
    assert nodes[print_call["dst_node_id"]]["node_type"] == "ExternalSymbol"


def test_dfg_stays_inside_lexical_scope(tmp_path: Path) -> None:
    result = parse(
        tmp_path,
        """\
x = 1
def f(x):
    y = x
    return y
z = x
""",
    )
    nodes = {event["node_id"]: event for event in result.nodes}
    dfg = [event for event in result.edges if event["edge_type"] == "DFG"]
    for event in dfg:
        source_scope = nodes[event["src_node_id"]]["scope"]
        destination_scope = nodes[event["dst_node_id"]]["scope"]
        assert source_scope == destination_scope


def test_reparse_same_content_has_identical_ids(tmp_path: Path) -> None:
    first = parse(tmp_path)
    second = parse(tmp_path)
    assert first.node_ids == second.node_ids
    assert first.edge_ids == second.edge_ids


def test_line_shift_does_not_change_graph_ids(tmp_path: Path) -> None:
    first = parse(tmp_path)
    second = parse(tmp_path, "\n\n" + SAMPLE)
    assert first.node_ids == second.node_ids
    assert first.edge_ids == second.edge_ids


def test_service_replay_and_incremental_delete_events(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    source = write_source(repo)
    sink = MemoryEventSink()
    service = ParserService(
        sink=sink,
        state_store=StateStore(tmp_path / "state"),
        repository="example/repo",
    )
    first = service.process_file(source, "sample.py")
    assert first.status == "succeeded"
    first_upserts = {
        event["node_id"]
        for _, _, event in sink.events
        if event["event_type"] == "node_upsert"
    }

    sink.events.clear()
    second = service.process_file(source, "sample.py")
    second_upserts = {
        event["node_id"]
        for _, _, event in sink.events
        if event["event_type"] == "node_upsert"
    }
    assert second.status == "succeeded"
    assert second.deleted_nodes == 0
    assert second.deleted_edges == 0
    assert second_upserts == first_upserts

    source.write_text(SAMPLE.replace("        print(result)\n", ""), encoding="utf-8")
    sink.events.clear()
    third = service.process_file(source, "sample.py")
    event_types = [event["event_type"] for _, _, event in sink.events]
    assert third.status == "succeeded"
    assert third.deleted_nodes > 0
    assert third.deleted_edges > 0
    assert "edge_delete" in event_types
    assert "node_delete" in event_types


def test_skip_unchanged_avoids_emitting_events(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    source = write_source(repo)
    sink = MemoryEventSink()
    state = StateStore(tmp_path / "state")
    service = ParserService(
        sink=sink, state_store=state, repository="example/repo", skip_unchanged=True
    )
    assert service.process_file(source, "sample.py").status == "succeeded"
    sink.events.clear()
    assert service.process_file(source, "sample.py").status == "skipped"
    assert sink.events == []


@pytest.mark.parametrize(
    ("content", "stage"),
    [("def broken(:\n", "ast_parse"), (b"\xff".decode("latin1"), "decode")],
)
def test_bad_file_emits_error_event(
    tmp_path: Path, content: str, stage: str
) -> None:
    source = write_source(tmp_path, content=content)
    if stage == "decode":
        source.write_bytes(b"\xff")
    sink = MemoryEventSink()
    service = ParserService(
        sink=sink,
        state_store=StateStore(tmp_path / "state"),
        repository="example/repo",
    )
    outcome = service.process_file(source, "sample.py")
    assert outcome.status == "failed"
    assert len(sink.events) == 1
    topic, key, event = sink.events[0]
    assert topic == ERROR_TOPIC
    assert key == "sample.py"
    assert event["stage"] == stage
    assert event["schema_version"] == SCHEMA_VERSION
    assert event["event_time"].endswith("Z")


def test_every_upsert_has_required_contract_fields(tmp_path: Path) -> None:
    result = parse(tmp_path)
    for event in [*result.nodes, *result.edges, result.metadata]:
        assert event["schema_version"] == SCHEMA_VERSION
        assert event["event_time"].endswith("Z")
        assert event["event_type"].endswith("_upsert")
        assert event["file_path"] == "sample.py"


def test_cli_dry_run_manifest(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    write_source(repo)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"kept_files": ["sample.py"]}), encoding="utf-8")
    output = tmp_path / "events.jsonl"
    state = tmp_path / "state"
    exit_code = main(
        [
            "--repo",
            str(repo),
            "--manifest",
            str(manifest),
            "--dry-run",
            "--output-jsonl",
            str(output),
            "--state-dir",
            str(state),
        ]
    )
    assert exit_code == 0
    envelopes = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert envelopes
    assert {item["topic"] for item in envelopes} >= {
        "cpg.node.events",
        "cpg.edge.events",
        "cpg.source.metadata.events",
    }
    for envelope in envelopes:
        assert isinstance(envelope["key"], str)
        assert envelope["value"]["schema_version"] == SCHEMA_VERSION
