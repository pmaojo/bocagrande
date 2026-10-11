"""The node types a pipeline can run.

Each node function takes ``(config, inputs, context)`` and returns ``(DataFrame or None, message)``. Sources
take no inputs, transformations take one (the merge join takes two, left then right) and destinations
return ``None``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from adapter.data_io import read_dataframe
from adapter.db_io import read_database, write_database
from adapter.reasoner_factory import DEFAULT_REASONER
from adapter.yaml_loader import load_schema
from bocagrande.formula_screen import check_formula
from bocagrande.pipeline.model import PipelineError
from bocagrande.transform import ETLStep, apply_transformations

NodeResult = Tuple[Optional[pd.DataFrame], str]
NodeFunction = Callable[[Dict[str, Any], List[pd.DataFrame], "Context"], NodeResult]
REGISTRY: Dict[str, NodeFunction] = {}
FILE_FORMATS = ("csv", "xlsx", "parquet", "json")
JOIN_TYPES = ("inner", "left", "right", "outer")


@dataclass
class Context:
    """Where a run happens. ``sandbox`` keeps every file path inside ``base_dir``."""

    base_dir: Path = field(default_factory=Path.cwd)
    sandbox: bool = False
    parameters: Dict[str, str] = field(default_factory=dict)
    allow_env: bool = True  # read ${NAME} from the environment in connection strings
    reasoner: str = DEFAULT_REASONER  # default for destination.graph nodes

    def resolve(self, raw: str) -> Path:
        path = (self.base_dir / raw).resolve() if not Path(raw).is_absolute() else Path(raw).resolve()
        if self.sandbox:
            try:
                path.relative_to(self.base_dir.resolve())
            except ValueError:
                raise PipelineError(f"The path '{raw}' is outside the working directory") from None
        return path


def node(type_name: str) -> Callable[[NodeFunction], NodeFunction]:
    def register(function: NodeFunction) -> NodeFunction:
        REGISTRY[type_name] = function
        return function
    return register


def _need(config: Dict[str, Any], *keys: str) -> None:
    for key in keys:
        if config.get(key) in (None, "", []):
            raise PipelineError(f"Missing '{key}'")


def _columns_exist(df: pd.DataFrame, names: List[str], what: str) -> None:
    missing = [n for n in names if n not in df.columns]
    if missing:
        raise PipelineError(f"{what}: column(s) not found: {', '.join(missing)}. Available: {', '.join(map(str, df.columns))}")


# ------------------------------------------------------------------ sources
@node("source.file")
def source_file(config, inputs, context):
    _need(config, "path")
    path = context.resolve(config["path"])
    dtype = {c: str for c in config.get("text_columns") or []} or None
    df = read_dataframe(str(path), dtype=dtype, read_options=config.get("options"))
    return df, f"read {len(df)} rows from {path.name}"


@node("source.database")
def source_database(config, inputs, context):
    df = read_database(config)
    return df, f"read {len(df)} rows from the database"


# ------------------------------------------------------------------ transformations
@node("transform.select")
def transform_select(config, inputs, context):
    """Keep, rename and drop columns. ``columns`` is ``[{name, rename?}]``; ``keep_others`` keeps the rest."""
    (df,) = inputs
    columns = config.get("columns") or []
    _columns_exist(df, [c["name"] for c in columns] + list(config.get("remove") or []), "select")
    remove = set(config.get("remove") or [])
    if columns:
        chosen = [c["name"] for c in columns]
        renames = {c["name"]: c["rename"] for c in columns if c.get("rename")}
        rest = [c for c in df.columns if c not in chosen and c not in remove] if config.get("keep_others") else []
        result = df[chosen + rest].rename(columns=renames)
    else:
        result = df.drop(columns=list(remove))
    return result, f"{len(result.columns)} columns"


@node("transform.mapping")
def transform_mapping(config, inputs, context):
    """Output columns defined by mapping steps (``ETLStep`` fields), including screened formulas."""
    (df,) = inputs
    _need(config, "steps")
    steps = [ETLStep(**s) for s in config["steps"]]
    result, generated, _, missing = apply_transformations(df, steps)
    return result, f"{len(generated)} generated, {len(missing)} missing" + (f" ({', '.join(missing)})" if missing else "")


@node("transform.constant")
def transform_constant(config, inputs, context):
    (df,) = inputs
    _need(config, "fields")
    result = df.copy()
    for f in config["fields"]:
        result[f["name"]] = f.get("value")
    return result, f"added {len(config['fields'])} constant column(s)"


@node("transform.sort")
def transform_sort(config, inputs, context):
    (df,) = inputs
    _need(config, "by")
    by = [b["column"] for b in config["by"]]
    _columns_exist(df, by, "sort")
    return df.sort_values(by=by, ascending=[bool(b.get("ascending", True)) for b in config["by"]], kind="stable"), "sorted"


@node("transform.filter")
def transform_filter(config, inputs, context):
    """Keep the rows where ``condition`` holds, e.g. ``edad >= 18 and pais == 'ES'`` (screened like formulas)."""
    (df,) = inputs
    _need(config, "condition")
    allowed, reason = check_formula(config["condition"])
    if not allowed:
        raise PipelineError(f"filter condition rejected: {reason}")
    try:
        result = df.query(config["condition"])
    except Exception as exc:
        raise PipelineError(f"filter condition failed: {exc}") from None
    return result, f"kept {len(result)} of {len(df)} rows"


@node("transform.merge_join")
def transform_merge_join(config, inputs, context):
    left, right = inputs
    _need(config, "keys_left", "keys_right")
    how = config.get("how", "inner")
    if how not in JOIN_TYPES:
        raise PipelineError(f"merge join: 'how' must be one of {', '.join(JOIN_TYPES)}")
    if len(config["keys_left"]) != len(config["keys_right"]):
        raise PipelineError("merge join: keys_left and keys_right must have the same length")
    _columns_exist(left, config["keys_left"], "merge join (left)")
    _columns_exist(right, config["keys_right"], "merge join (right)")
    # nullable dtypes keep integers as integers when a join leaves some rows without a match (2 stays 2, not 2.0)
    result = left.convert_dtypes().merge(right.convert_dtypes(), how=how, left_on=config["keys_left"],
                                         right_on=config["keys_right"], suffixes=("", "_right"))
    return result, f"{len(result)} rows"


# ------------------------------------------------------------------ destinations
@node("destination.database")
def destination_database(config, inputs, context):
    (df,) = inputs
    write_database(df, config)
    return None, f"wrote {len(df)} rows to '{config.get('table_name') or config.get('collection_name')}'"


@node("destination.file")
def destination_file(config, inputs, context):
    (df,) = inputs
    _need(config, "path")
    path = context.resolve(config["path"])
    fmt = (config.get("format") or path.suffix.lstrip(".")).lower()
    if fmt not in FILE_FORMATS:
        raise PipelineError(f"Unsupported file format '{fmt}'. Use one of: {', '.join(FILE_FORMATS)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "xlsx":
        df.to_excel(path, index=False)
    elif fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_json(path, orient="records", force_ascii=False, indent=2)
    return None, f"wrote {len(df)} rows to {path.name}"


@node("destination.graph")
def destination_graph(config, inputs, context):
    """Build the OWL graph from a schema, reason over it, optionally validate it with SHACL, and write Turtle."""
    from adapter.decision_log import create_decisions
    from adapter.reasoner_factory import create_reasoner
    from bocagrande.validation import validate_dataframe_report
    from ontology.service import OntologyBuilder
    from ontology.tbox_builder import build_global_tbox

    (df,) = inputs
    _need(config, "schema", "output")
    schema_path = context.resolve(config["schema"])
    schema = load_schema(str(schema_path))
    if schema is None:
        raise PipelineError(f"Invalid schema: {config['schema']}")
    output = context.resolve(config["output"])

    reasoner = create_reasoner(config.get("reasoner", context.reasoner))
    builder = OntologyBuilder(build_global_tbox(schema_path.parent), reasoner=reasoner)
    graph = builder.build_abox_graph(schema, df)
    notes = [f"{len(graph)} triples"]
    if reasoner is not None:
        ok, logs = builder.reason_graph(graph)
        if not ok:
            raise PipelineError(f"The reasoner found the data inconsistent: {logs.strip().splitlines()[0] if logs.strip() else ''}")
        notes.append("consistent")
    if config.get("validate"):
        report = validate_dataframe_report(df, schema, reasoner=None, decisions=create_decisions("pipeline"))
        notes.append("SHACL conforms" if report.shacl_ok else f"SHACL: {len(report.shacl.violations)} violation(s)")
        if not report.shacl_ok and config.get("fail_on_violations"):
            causes = ", ".join(sorted({t.category for t in report.triage}))
            raise PipelineError(f"SHACL validation failed ({len(report.shacl.violations)} violations; likely causes: {causes})")
    output.parent.mkdir(parents=True, exist_ok=True)
    graph.serialize(destination=str(output), format="turtle")
    return None, "; ".join(notes) + f"; wrote {output.name}"


__all__ = ["Context", "REGISTRY", "node", "FILE_FORMATS", "JOIN_TYPES"]
