"""A pipeline: nodes (sources, transformations, destinations) joined by edges.

The format is plain data, so it round-trips through YAML or JSON and is what the pipeline editor and the
Pentaho importer produce. Node types are ``<kind>.<name>``, for example ``source.database`` or
``transform.select``; a type starting with ``unsupported.`` keeps a step that could not be converted so it
stays visible, but it cannot run.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml

SOURCE, TRANSFORM, DESTINATION, UNSUPPORTED = "source", "transform", "destination", "unsupported"
KINDS = (SOURCE, TRANSFORM, DESTINATION, UNSUPPORTED)
MERGE_JOIN = "transform.merge_join"
JOIN_HANDLES = ("left", "right")


class PipelineError(Exception):
    """The pipeline is invalid or a node failed."""


@dataclass
class Node:
    id: str
    type: str
    config: Dict[str, Any] = field(default_factory=dict)
    label: str = ""
    position: Dict[str, float] = field(default_factory=lambda: {"x": 0, "y": 0})

    @property
    def kind(self) -> str:
        return self.type.split(".", 1)[0]


@dataclass
class Edge:
    source: str
    target: str
    handle: Optional[str] = None  # "left" / "right" for the two inputs of a merge join


@dataclass
class Pipeline:
    name: str = ""
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)
    parameters: Dict[str, str] = field(default_factory=dict)
    description: str = ""

    def node(self, node_id: str) -> Node:
        for n in self.nodes:
            if n.id == node_id:
                return n
        raise PipelineError(f"Unknown node '{node_id}'")

    def inputs_of(self, node_id: str) -> List[Edge]:
        return [e for e in self.edges if e.target == node_id]

    def outputs_of(self, node_id: str) -> List[Edge]:
        return [e for e in self.edges if e.source == node_id]

    def execution_order(self) -> List[Node]:
        """Nodes in dependency order; raises :class:`PipelineError` on a cycle."""
        by_id = {n.id: n for n in self.nodes}
        pending = {n.id: 0 for n in self.nodes}
        for e in self.edges:
            if e.source in by_id and e.target in by_id:
                pending[e.target] += 1
        ready = deque(n.id for n in self.nodes if pending[n.id] == 0)
        order: List[Node] = []
        while ready:
            current = ready.popleft()
            order.append(by_id[current])
            for e in self.outputs_of(current):
                if e.target in pending:
                    pending[e.target] -= 1
                    if pending[e.target] == 0:
                        ready.append(e.target)
        if len(order) != len(self.nodes):
            raise PipelineError("The pipeline has a cycle")
        return order

    def validate(self) -> List[str]:
        """Every structural problem found, as readable messages; an empty list means the pipeline is valid."""
        problems: List[str] = []
        ids = [n.id for n in self.nodes]
        for dup in sorted({i for i in ids if ids.count(i) > 1}):
            problems.append(f"Node id '{dup}' is used more than once")
        known = set(ids)
        for e in self.edges:
            for end in (e.source, e.target):
                if end not in known:
                    problems.append(f"Edge {e.source} -> {e.target} refers to the unknown node '{end}'")
        for n in self.nodes:
            if not n.id:
                problems.append("A node has no id")
            if n.kind not in KINDS:
                problems.append(f"Node '{n.id}' has the type '{n.type}': it must start with one of {', '.join(KINDS)}")
            incoming, outgoing = self.inputs_of(n.id), self.outputs_of(n.id)
            if n.kind == SOURCE and incoming:
                problems.append(f"Source '{n.id}' cannot have an input")
            if n.kind == DESTINATION and outgoing:
                problems.append(f"Destination '{n.id}' cannot have an output")
            if n.kind == DESTINATION and len(incoming) != 1:
                problems.append(f"Destination '{n.id}' needs exactly one input, it has {len(incoming)}")
            if n.kind == TRANSFORM and n.type == MERGE_JOIN:
                handles = sorted(e.handle or "" for e in incoming)
                if handles != sorted(JOIN_HANDLES):
                    problems.append(f"Merge join '{n.id}' needs one input on 'left' and one on 'right'")
            elif n.kind == TRANSFORM and len(incoming) != 1:
                problems.append(f"Transformation '{n.id}' needs exactly one input, it has {len(incoming)}")
        if not problems:
            try:
                self.execution_order()
            except PipelineError as exc:
                problems.append(str(exc))
        return problems

    # ---- (de)serialisation
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name, "description": self.description, "parameters": dict(self.parameters),
            "nodes": [{"id": n.id, "type": n.type, "label": n.label, "position": dict(n.position), "config": n.config}
                      for n in self.nodes],
            "edges": [{"source": e.source, "target": e.target, **({"handle": e.handle} if e.handle else {})}
                      for e in self.edges],
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Pipeline":
        if not isinstance(data, dict):
            raise PipelineError("A pipeline must be a mapping with 'nodes' and 'edges'")
        try:
            nodes = [Node(id=str(n["id"]), type=str(n["type"]), config=dict(n.get("config") or {}),
                          label=str(n.get("label") or ""), position=dict(n.get("position") or {"x": 0, "y": 0}))
                     for n in data.get("nodes") or []]
            edges = [Edge(source=str(e["source"]), target=str(e["target"]), handle=e.get("handle") or e.get("targetHandle"))
                     for e in data.get("edges") or []]
        except (KeyError, TypeError) as exc:
            raise PipelineError(f"Malformed pipeline: {exc!r}") from None
        return Pipeline(name=str(data.get("name") or ""), nodes=nodes, edges=edges,
                        parameters={str(k): str(v) for k, v in (data.get("parameters") or {}).items()},
                        description=str(data.get("description") or ""))

    def to_yaml(self) -> str:
        return yaml.safe_dump(self.to_dict(), allow_unicode=True, sort_keys=False)

    @staticmethod
    def from_yaml(text: str) -> "Pipeline":
        try:
            return Pipeline.from_dict(yaml.safe_load(text))
        except yaml.YAMLError as exc:
            raise PipelineError(f"Invalid YAML: {exc}") from None


__all__ = ["Pipeline", "Node", "Edge", "PipelineError", "KINDS", "SOURCE", "TRANSFORM", "DESTINATION", "UNSUPPORTED",
           "MERGE_JOIN", "JOIN_HANDLES"]
