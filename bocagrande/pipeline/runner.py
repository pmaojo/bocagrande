"""Run a pipeline and report what each node did."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from bocagrande.pipeline.model import MERGE_JOIN, Pipeline, PipelineError, UNSUPPORTED
from bocagrande.pipeline.nodes import Context, REGISTRY

logger = logging.getLogger(__name__)

OK, FAILED, SKIPPED = "ok", "failed", "skipped"
_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")


@dataclass
class NodeReport:
    id: str
    type: str
    status: str
    message: str = ""
    rows: Optional[int] = None
    columns: List[str] = field(default_factory=list)


@dataclass
class RunReport:
    pipeline: str
    nodes: List[NodeReport] = field(default_factory=list)

    @property
    def status(self) -> str:
        return OK if all(n.status == OK for n in self.nodes) else FAILED

    @property
    def failed(self) -> List[NodeReport]:
        return [n for n in self.nodes if n.status == FAILED]

    def to_dict(self) -> Dict[str, Any]:
        return {"pipeline": self.pipeline, "status": self.status,
                "nodes": [{"id": n.id, "type": n.type, "status": n.status, "message": n.message, "rows": n.rows,
                           "columns": n.columns} for n in self.nodes]}


def _resolve_references(value: Any, context: Context, key: str = "") -> Any:
    """Replace ``${NAME}`` in every string of a node's config.

    Names come from the pipeline's parameters. Environment variables are read only inside
    ``connection_string`` and only when the context allows it, so a pipeline from outside cannot copy
    other secrets into, say, a file path.
    """
    if isinstance(value, dict):
        return {k: _resolve_references(v, context, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_references(v, context, key) for v in value]
    if not isinstance(value, str) or "${" not in value:
        return value

    def replace(match: re.Match) -> str:
        name = match.group(1)
        if name in context.parameters:
            return context.parameters[name]
        if key == "connection_string" and context.allow_env:
            return match.group(0)  # adapter.db_io reads the environment
        raise PipelineError(f"'${{{name}}}' is not a parameter of this pipeline")

    return _REFERENCE.sub(replace, value)


def run_pipeline(pipeline: Pipeline, context: Optional[Context] = None) -> RunReport:
    """Run every node in dependency order. A failed node skips everything downstream of it."""
    context = context or Context()
    context.parameters = {**pipeline.parameters, **context.parameters}
    problems = pipeline.validate()
    if problems:
        raise PipelineError("Invalid pipeline: " + "; ".join(problems))

    report = RunReport(pipeline=pipeline.name)
    frames: Dict[str, pd.DataFrame] = {}
    broken: set = set()

    for current in pipeline.execution_order():
        incoming = pipeline.inputs_of(current.id)
        if any(e.source in broken for e in incoming):
            broken.add(current.id)
            report.nodes.append(NodeReport(current.id, current.type, SKIPPED, "an earlier node failed"))
            continue
        try:
            if current.kind == UNSUPPORTED or current.type not in REGISTRY:
                raise PipelineError(f"'{current.type}' is not supported and cannot run")
            if current.type == MERGE_JOIN:
                by_handle = {e.handle: frames[e.source] for e in incoming}
                inputs = [by_handle["left"], by_handle["right"]]
            else:
                inputs = [frames[e.source] for e in incoming]
            frame, message = REGISTRY[current.type](_resolve_references(current.config, context), inputs, context)
        except Exception as exc:  # a node failing must not stop the report
            broken.add(current.id)
            logger.warning("Node %s failed: %s", current.id, exc)
            report.nodes.append(NodeReport(current.id, current.type, FAILED, str(exc) or type(exc).__name__))
            continue
        if frame is not None:
            frames[current.id] = frame
        report.nodes.append(NodeReport(current.id, current.type, OK, message,
                                       None if frame is None else len(frame),
                                       [] if frame is None else [str(c) for c in frame.columns]))
    return report


def load_pipeline(path: str | Path) -> Pipeline:
    return Pipeline.from_yaml(Path(path).read_text(encoding="utf-8"))


__all__ = ["run_pipeline", "load_pipeline", "RunReport", "NodeReport", "OK", "FAILED", "SKIPPED"]
