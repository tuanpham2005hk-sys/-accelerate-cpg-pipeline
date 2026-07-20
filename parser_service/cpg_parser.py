"""Python ``ast`` to a compact Code Property Graph event model."""

from __future__ import annotations

import ast
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from . import __version__
from .events import base_event, utc_event_time
from .identifiers import (
    content_sha256,
    edge_id,
    external_symbol_id,
    file_id,
    node_id,
    stable_hash,
)


@dataclass(slots=True)
class ParseResult:
    repository: str
    file_path: str
    content_hash: str
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    metadata: dict[str, Any]

    @property
    def node_ids(self) -> set[str]:
        return {item["node_id"] for item in self.nodes}

    @property
    def edge_ids(self) -> set[str]:
        return {item["edge_id"] for item in self.edges}


class CPGParser:
    """Parse one Python file at a time into AST/CFG/DFG/Call events.

    The object holds only the current file's AST and indexes. Callers should
    create/consume a :class:`ParseResult`, publish it, then move to the next
    file to preserve bounded-memory behavior.
    """

    def __init__(self, repository: str = "huggingface/accelerate") -> None:
        self.repository = repository

    def parse_file(self, path: Path, file_path: str) -> ParseResult:
        started = time.perf_counter()
        raw = path.read_bytes()
        digest = content_sha256(raw)
        source = raw.decode("utf-8")
        tree = ast.parse(source, filename=file_path, type_comments=True)
        event_time = utc_event_time()

        builder = _GraphBuilder(self.repository, file_path, event_time)
        nodes, edges = builder.build(tree)
        edge_counts = Counter(edge["edge_type"] for edge in edges)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)

        metadata = {
            **base_event("source_upsert", event_time),
            "file_id": file_id(self.repository, file_path),
            "file_path": file_path,
            "repository": self.repository,
            "language": "python",
            "loc": source.count("\n") + (0 if source.endswith("\n") or not source else 1),
            "size_bytes": len(raw),
            "content_sha256": digest,
            "parser": "python-ast",
            "parser_service_version": __version__,
            "parse_duration_ms": elapsed_ms,
            "counts": {
                "nodes": len(nodes),
                "edges": len(edges),
                "ast_child_edges": edge_counts["AST_CHILD"],
                "cfg_edges": edge_counts["CFG"],
                "dfg_edges": edge_counts["DFG"],
                "call_edges": edge_counts["CALL"],
            },
        }
        return ParseResult(
            repository=self.repository,
            file_path=file_path,
            content_hash=digest,
            nodes=nodes,
            edges=edges,
            metadata=metadata,
        )


class _GraphBuilder:
    def __init__(self, repository: str, file_path: str, event_time: str) -> None:
        self.repository = repository
        self.file_path = file_path
        self.event_time = event_time
        self.ids: dict[int, str] = {}
        self.scopes: dict[int, str] = {}
        self.owners: dict[int, str] = {}
        self.raw_nodes: list[dict[str, Any]] = []
        self.edges_by_id: dict[str, dict[str, Any]] = {}
        self.occurrences: defaultdict[tuple[str, str, str], int] = defaultdict(int)
        self.definitions: defaultdict[str, list[str]] = defaultdict(list)
        self.executable_scopes: list[ast.AST] = []
        self.external_nodes: dict[str, str] = {}

    def build(self, tree: ast.Module) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        self._index_ast(tree, scope="<module>", owner=None)
        self._build_cfg()
        self._build_dfg()
        self._build_calls(tree)
        return self.raw_nodes, list(self.edges_by_id.values())

    # ------------------------------------------------------------------ AST
    def _index_ast(self, node: ast.AST, scope: str, owner: str | None) -> str:
        kind = type(node).__name__
        signature = self._node_signature(node)
        occurrence_key = (scope, kind, signature)
        occurrence = self.occurrences[occurrence_key]
        self.occurrences[occurrence_key] += 1
        identifier = node_id(
            self.repository,
            self.file_path,
            scope,
            kind,
            signature,
            occurrence,
        )
        self.ids[id(node)] = identifier
        self.scopes[id(node)] = scope

        if isinstance(node, ast.Module):
            owner = identifier
            self.executable_scopes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            owner = identifier
            self.executable_scopes.append(node)
        elif isinstance(node, ast.ClassDef):
            self.executable_scopes.append(node)
        if owner is not None:
            self.owners[id(node)] = owner

        name = self._display_name(node)
        properties: dict[str, Any] = {"language": "python"}
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            qualified = self._qualified(scope, node.name)
            properties["qualified_name"] = qualified
            properties["decorators"] = [self._expression_name(d) for d in node.decorator_list]
            self.definitions[qualified].append(identifier)
            if qualified != node.name:
                self.definitions[node.name].append(identifier)
        elif isinstance(node, ast.Constant):
            properties["value_type"] = type(node.value).__name__
            properties["value_preview"] = repr(node.value)[:160]
        elif isinstance(node, ast.Name):
            properties["context"] = type(node.ctx).__name__

        self.raw_nodes.append(
            {
                **base_event("node_upsert", self.event_time),
                "node_id": identifier,
                "node_type": kind,
                "name": name,
                "file_path": self.file_path,
                "scope": scope,
                "start_line": getattr(node, "lineno", None),
                "end_line": getattr(node, "end_lineno", None),
                "start_col": getattr(node, "col_offset", None),
                "end_col": getattr(node, "end_col_offset", None),
                "properties": properties,
            }
        )

        child_scope = scope
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            child_scope = self._qualified(scope, node.name)
        elif isinstance(node, ast.Lambda):
            child_scope = self._qualified(scope, f"<lambda:{getattr(node, 'lineno', 0)}>")

        for field, value in ast.iter_fields(node):
            if isinstance(value, ast.AST):
                child_id = self._index_ast(value, child_scope, owner)
                self._add_edge(
                    "AST_CHILD",
                    identifier,
                    child_id,
                    anchor=field,
                    properties={"field": field},
                )
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    if not isinstance(child, ast.AST):
                        continue
                    child_id = self._index_ast(child, child_scope, owner)
                    self._add_edge(
                        "AST_CHILD",
                        identifier,
                        child_id,
                        anchor=f"{field}:{index}",
                        properties={"field": field, "index": index},
                    )
        return identifier

    def _node_signature(self, node: ast.AST) -> str:
        if isinstance(node, ast.Module):
            return "module"
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return node.name
        if isinstance(node, ast.arg):
            return node.arg
        if isinstance(node, ast.Name):
            return f"{node.id}:{type(node.ctx).__name__}"
        if isinstance(node, ast.Attribute):
            return f"attribute:{node.attr}"
        if isinstance(node, ast.alias):
            return f"{node.name}:{node.asname or ''}"
        if isinstance(node, ast.Constant):
            return f"constant:{type(node.value).__name__}:{repr(node.value)[:120]}"
        normalized = ast.dump(node, annotate_fields=True, include_attributes=False)
        return stable_hash(normalized, prefix="ast")

    @staticmethod
    def _display_name(node: ast.AST) -> str | None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return node.name
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.arg):
            return node.arg
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.alias):
            return node.asname or node.name
        if isinstance(node, ast.Call):
            return _GraphBuilder._expression_name(node.func)
        if isinstance(node, ast.Module):
            return Path("module.py").stem
        return None

    @staticmethod
    def _qualified(scope: str, name: str) -> str:
        return name if scope == "<module>" else f"{scope}.{name}"

    # ------------------------------------------------------------------ CFG
    def _build_cfg(self) -> None:
        for scope_node in self.executable_scopes:
            body = getattr(scope_node, "body", None)
            if isinstance(body, list):
                self._cfg_block(body, follow=None, break_target=None, continue_target=None)

    def _cfg_block(
        self,
        statements: list[ast.stmt],
        follow: str | None,
        break_target: str | None,
        continue_target: str | None,
    ) -> str | None:
        current_follow = follow
        for statement in reversed(statements):
            sid = self.ids[id(statement)]
            if isinstance(statement, ast.If):
                true_entry = self._cfg_block(
                    statement.body, current_follow, break_target, continue_target
                )
                false_entry = self._cfg_block(
                    statement.orelse, current_follow, break_target, continue_target
                )
                self._cfg_link(sid, true_entry or current_follow, "TRUE")
                self._cfg_link(sid, false_entry or current_follow, "FALSE")
            elif isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
                exit_entry = self._cfg_block(
                    statement.orelse, current_follow, break_target, continue_target
                )
                body_entry = self._cfg_block(
                    statement.body,
                    sid,
                    break_target=current_follow,
                    continue_target=sid,
                )
                self._cfg_link(sid, body_entry or sid, "LOOP_BODY")
                self._cfg_link(sid, exit_entry or current_follow, "LOOP_EXIT")
            elif isinstance(statement, (ast.With, ast.AsyncWith)):
                body_entry = self._cfg_block(
                    statement.body, current_follow, break_target, continue_target
                )
                self._cfg_link(sid, body_entry or current_follow, "ENTER")
            elif isinstance(statement, ast.Try):
                final_entry = self._cfg_block(
                    statement.finalbody, current_follow, break_target, continue_target
                )
                normal_follow = self._cfg_block(
                    statement.orelse,
                    final_entry or current_follow,
                    break_target,
                    continue_target,
                )
                body_entry = self._cfg_block(
                    statement.body,
                    normal_follow or final_entry or current_follow,
                    break_target,
                    continue_target,
                )
                self._cfg_link(sid, body_entry or normal_follow or final_entry, "TRY")
                for handler in statement.handlers:
                    handler_id = self.ids[id(handler)]
                    handler_entry = self._cfg_block(
                        handler.body,
                        final_entry or current_follow,
                        break_target,
                        continue_target,
                    )
                    self._cfg_link(sid, handler_id, "EXCEPT")
                    self._cfg_link(handler_id, handler_entry or final_entry or current_follow, "HANDLE")
            elif isinstance(statement, ast.Return):
                pass
            elif isinstance(statement, ast.Raise):
                pass
            elif isinstance(statement, ast.Break):
                self._cfg_link(sid, break_target, "BREAK")
            elif isinstance(statement, ast.Continue):
                self._cfg_link(sid, continue_target, "CONTINUE")
            else:
                self._cfg_link(sid, current_follow, "NEXT")
            current_follow = sid
        return current_follow

    def _cfg_link(self, source: str, target: str | None, label: str) -> None:
        if target is not None:
            self._add_edge("CFG", source, target, anchor=label, properties={"branch": label})

    # ------------------------------------------------------------------ DFG
    def _build_dfg(self) -> None:
        for scope_node in self.executable_scopes:
            body = getattr(scope_node, "body", None)
            if not isinstance(body, list):
                continue
            last_definitions: dict[str, str] = {}
            if isinstance(scope_node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                for argument in self._function_arguments(scope_node.args):
                    last_definitions[argument.arg] = self.ids[id(argument)]

            statements = sorted(
                self._scope_statements(body),
                key=lambda item: (getattr(item, "lineno", 0), getattr(item, "col_offset", 0)),
            )
            for statement in statements:
                direct = list(self._direct_expression_nodes(statement))
                loads = [
                    item
                    for item in direct
                    if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Load)
                ]
                for load in sorted(loads, key=self._source_position):
                    source = last_definitions.get(load.id)
                    if source is not None:
                        self._add_edge(
                            "DFG",
                            source,
                            self.ids[id(load)],
                            anchor=load.id,
                            properties={"variable": load.id, "flow": "DEF_USE"},
                        )

                if isinstance(statement, ast.AugAssign) and isinstance(
                    statement.target, ast.Name
                ):
                    source = last_definitions.get(statement.target.id)
                    if source is not None:
                        self._add_edge(
                            "DFG",
                            source,
                            self.ids[id(statement.target)],
                            anchor=f"aug:{statement.target.id}",
                            properties={
                                "variable": statement.target.id,
                                "flow": "DEF_USE",
                            },
                        )

                stores = [
                    item
                    for item in direct
                    if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Store)
                ]
                for store in sorted(stores, key=self._source_position):
                    last_definitions[store.id] = self.ids[id(store)]

                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    last_definitions[statement.name] = self.ids[id(statement)]
                elif isinstance(statement, (ast.Import, ast.ImportFrom)):
                    for alias in statement.names:
                        defined_name = alias.asname or alias.name.split(".")[0]
                        last_definitions[defined_name] = self.ids[id(statement)]

    def _scope_statements(self, body: list[ast.stmt]) -> Iterable[ast.stmt]:
        for statement in body:
            yield statement
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            for _, value in ast.iter_fields(statement):
                if isinstance(value, list):
                    nested = [item for item in value if isinstance(item, ast.stmt)]
                    yield from self._scope_statements(nested)

    def _direct_expression_nodes(self, root: ast.stmt) -> Iterable[ast.AST]:
        def visit(node: ast.AST, is_root: bool = False) -> Iterable[ast.AST]:
            yield node
            if not is_root and isinstance(
                node, (ast.stmt, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
            ):
                return
            for child in ast.iter_child_nodes(node):
                yield from visit(child)

        return visit(root, is_root=True)

    @staticmethod
    def _function_arguments(arguments: ast.arguments) -> list[ast.arg]:
        result = [*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs]
        if arguments.vararg is not None:
            result.append(arguments.vararg)
        if arguments.kwarg is not None:
            result.append(arguments.kwarg)
        return result

    @staticmethod
    def _source_position(node: ast.AST) -> tuple[int, int]:
        return (getattr(node, "lineno", 0), getattr(node, "col_offset", 0))

    # ----------------------------------------------------------------- Calls
    def _build_calls(self, tree: ast.AST) -> None:
        for call in (item for item in ast.walk(tree) if isinstance(item, ast.Call)):
            caller = self.owners.get(id(call))
            if caller is None:
                continue
            symbol = self._expression_name(call.func) or "<dynamic-call>"
            destination = self._resolve_symbol(symbol, self.scopes.get(id(call), "<module>"))
            if destination is None:
                destination = self._external_node(symbol)
            call_site = self.ids[id(call)]
            self._add_edge(
                "CALL",
                caller,
                destination,
                anchor=call_site,
                properties={
                    "target": symbol,
                    "call_site_node_id": call_site,
                    "call_line": getattr(call, "lineno", None),
                },
            )

    def _resolve_symbol(self, symbol: str, scope: str) -> str | None:
        candidates: list[str] = []
        if symbol.startswith("self.") or symbol.startswith("cls."):
            method = symbol.split(".", 1)[1]
            class_scope = scope.rsplit(".", 1)[0] if "." in scope else ""
            if class_scope:
                candidates.append(f"{class_scope}.{method}")
        elif "." not in symbol:
            current = scope
            while current and current != "<module>":
                candidates.append(f"{current}.{symbol}")
                current = current.rsplit(".", 1)[0] if "." in current else "<module>"
            candidates.append(symbol)
        else:
            candidates.append(symbol)

        for candidate in candidates:
            values = self.definitions.get(candidate, [])
            if len(values) == 1:
                return values[0]
        simple = symbol.rsplit(".", 1)[-1]
        values = self.definitions.get(simple, [])
        return values[0] if len(values) == 1 else None

    def _external_node(self, symbol: str) -> str:
        if symbol in self.external_nodes:
            return self.external_nodes[symbol]
        identifier = external_symbol_id(self.repository, self.file_path, symbol)
        self.external_nodes[symbol] = identifier
        self.raw_nodes.append(
            {
                **base_event("node_upsert", self.event_time),
                "node_id": identifier,
                "node_type": "ExternalSymbol",
                "name": symbol,
                "file_path": self.file_path,
                "scope": "<external>",
                "start_line": None,
                "end_line": None,
                "start_col": None,
                "end_col": None,
                "properties": {"language": "python", "resolved": False},
            }
        )
        return identifier

    @staticmethod
    def _expression_name(expression: ast.AST) -> str:
        if isinstance(expression, ast.Name):
            return expression.id
        if isinstance(expression, ast.Attribute):
            prefix = _GraphBuilder._expression_name(expression.value)
            return f"{prefix}.{expression.attr}" if prefix else expression.attr
        if isinstance(expression, ast.Call):
            prefix = _GraphBuilder._expression_name(expression.func)
            return f"{prefix}()" if prefix else "<call-result>"
        if isinstance(expression, ast.Subscript):
            return _GraphBuilder._expression_name(expression.value)
        return ""

    # --------------------------------------------------------------- Edges
    def _add_edge(
        self,
        edge_type: str,
        source: str,
        destination: str,
        *,
        anchor: str = "",
        properties: dict[str, Any] | None = None,
    ) -> None:
        identifier = edge_id(edge_type, source, destination, anchor)
        self.edges_by_id[identifier] = {
            **base_event("edge_upsert", self.event_time),
            "edge_id": identifier,
            "edge_type": edge_type,
            "src_node_id": source,
            "dst_node_id": destination,
            "file_path": self.file_path,
            "properties": properties or {},
        }
