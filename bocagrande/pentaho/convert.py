"""Convert a Kettle transformation into a Bocagrande pipeline, and say what could not be converted.

A step that is not understood becomes an ``unsupported.<Type>`` node: it stays in the pipeline, visible and with
its (non-sensitive) settings, but does not run. Nothing is silently dropped or approximated: a step is converted
completely or reported. Passwords are never copied; each connection gets a ``${<NAME>_PASSWORD}`` reference that
the person running the pipeline provides in the environment.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

from bocagrande.pentaho.ktr import Connection, Step, Transformation, safe_config, text_of
from bocagrande.pipeline import Edge, Node, Pipeline
from bocagrande.pipeline.model import MERGE_JOIN

_VARIABLE = re.compile(r"(\$\{[^}]*\})")
_NUMBER = re.compile(r"^[+-]?\d+(\.\d+)?$")
JOIN_TYPES = {"INNER": "inner", "LEFT OUTER": "left", "RIGHT OUTER": "right", "FULL OUTER": "outer"}
CALCULATIONS = {"ADD": "{a} + {b}", "SUBTRACT": "{a} - {b}", "MULTIPLY": "{a} * {b}", "DIVIDE": "{a} / {b}",
                "COPY_OF_FIELD": "{a}"}
COMPARISONS = {"=": "==", "<>": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
LOGIC = {"AND": "and", "OR": "or"}


class Unsupported(Exception):
    """A step cannot be converted; the message says why."""


@dataclass
class ConversionReport:
    converted: List[str] = field(default_factory=list)
    unsupported: List[Dict[str, str]] = field(default_factory=list)  # {step, type, reason}
    warnings: List[str] = field(default_factory=list)
    environment: List[str] = field(default_factory=list)  # variables the person running it must set

    @property
    def complete(self) -> bool:
        return not self.unsupported

    def to_dict(self) -> Dict[str, object]:
        return {"converted": self.converted, "unsupported": self.unsupported, "warnings": self.warnings,
                "environment": self.environment, "complete": self.complete}


@dataclass
class _Converted:
    nodes: List[Node]  # first receives the incoming edges, last emits the outgoing ones


@dataclass
class _Context:
    transformation: Transformation
    report: ConversionReport
    ids: Dict[str, str]
    incoming: Dict[str, List[str]]


def slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower() or "step"


def _quote_keeping_variables(value: str) -> str:
    return "".join(part if part.startswith("${") else quote(part, safe="") for part in _VARIABLE.split(value))


def connection_url(connection: Connection, report: ConversionReport) -> str:
    """A SQLAlchemy URL for a Kettle connection, with the password as an environment reference."""
    kind = connection.type
    user = _quote_keeping_variables(connection.username)
    password_variable = f"{slug(connection.name).upper()}_PASSWORD"
    host = _quote_keeping_variables(connection.host or "localhost")
    database = _quote_keeping_variables(connection.database)
    port = f":{connection.port}" if connection.port else ""

    if kind == "SQLITE":
        return f"sqlite:///{connection.database}"
    schemes = {"POSTGRESQL": "postgresql+psycopg", "MYSQL": "mysql+pymysql", "MARIADB": "mysql+pymysql",
               "MSSQL": "mssql+pyodbc", "MSSQLNATIVE": "mssql+pyodbc", "ORACLE": "oracle+oracledb"}
    if kind not in schemes:
        raise Unsupported(f"the database type '{kind or 'unknown'}' of connection '{connection.name}' is not supported")
    userinfo = ""
    if user:
        report.environment.append(password_variable) if password_variable not in report.environment else None
        userinfo = f"{user}:${{{password_variable}}}@"
    url = f"{schemes[kind]}://{userinfo}{host}{port}"
    if kind == "ORACLE":
        return f"{url}/?service_name={database}"
    url += f"/{database}"
    if kind in ("MSSQL", "MSSQLNATIVE"):
        url += "?driver=ODBC+Driver+18+for+SQL+Server"
    return url


# ------------------------------------------------------------------ conditions (FilterRows)
def _column(name: str) -> str:
    return f"`{name}`" if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) else name


def _literal(value: Optional[object]) -> str:
    kind = text_of(value, "type") if value is not None else ""
    text = text_of(value, "text") if value is not None else ""
    if kind == "String":
        return repr(text)
    if kind in ("Integer", "Number", "BigNumber") and _NUMBER.match(text):
        return text
    if kind == "Boolean":
        return "True" if text.lower() in ("y", "true") else "False"
    raise Unsupported(f"the constant of type '{kind}' in a condition is not supported")


def _condition(element) -> str:
    """A Kettle condition (leaf or nested) as a pandas expression."""
    children = element.findall("./conditions/condition")
    if children:
        parts = []
        for index, child in enumerate(children):
            operator = text_of(child, "operator", "-")
            expression = _condition(child)
            if index == 0:
                parts.append(expression)
            elif operator in LOGIC:
                parts.append(f"{LOGIC[operator]} {expression}")
            else:
                raise Unsupported(f"the logical operator '{operator}' in a condition is not supported")
        expression = "(" + " ".join(parts) + ")"
    else:
        left = text_of(element, "leftvalue")
        function = text_of(element, "function")
        right_field = text_of(element, "rightvalue")
        value = element.find("value")
        if not left:
            raise Unsupported("a condition without a left field")
        if function in COMPARISONS:
            right = _column(right_field) if right_field else _literal(value)
            expression = f"({_column(left)} {COMPARISONS[function]} {right})"
        elif function in ("CONTAINS", "STARTS WITH", "ENDS WITH") and not right_field:
            method = {"CONTAINS": "contains", "STARTS WITH": "startswith", "ENDS WITH": "endswith"}[function]
            expression = f"({_column(left)}.str.{method}({_literal(value)}))"
        else:
            raise Unsupported(f"the comparison '{function}' in a condition is not supported")
    return f"not {expression}" if text_of(element, "negated", "N").upper() == "Y" else expression


# ------------------------------------------------------------------ step converters
def _node(step: Step, context: _Context, type_: str, config: Dict[str, object], suffix: str = "") -> Node:
    node_id = context.ids[step.name] + suffix
    return Node(id=node_id, type=type_, label=step.name if not suffix else f"{step.name} (fields)", config=config,
                position={"x": step.x, "y": step.y})


def table_input(step: Step, context: _Context) -> _Converted:
    if context.incoming.get(step.name):
        raise Unsupported("a Table input that receives rows from another step (one query per row)")
    if step.flag("execute_each_row"):
        raise Unsupported("'Execute for each row' queries are not supported")
    connection = context.transformation.connections.get(step.text("connection"))
    if connection is None:
        raise Unsupported(f"the connection '{step.text('connection')}' is not defined in the file")
    sql = step.text("sql")
    if "?" in sql and not step.flag("variables_active"):
        context.report.warnings.append(f"'{step.name}': the query has '?' placeholders that Kettle fills from another step")
    return _Converted([_node(step, context, "source.database",
                             {"connection_string": connection_url(connection, context.report), "query": sql})])


def _file_source(step: Step, context: _Context, path: str, separator: str, header: bool, names: List[str]) -> _Converted:
    if not path:
        raise Unsupported("no file name")
    options: Dict[str, object] = {}
    if separator:
        options["sep"] = separator
    encoding = step.text("encoding")
    if encoding:
        options["encoding"] = encoding
    if not header:
        options["header"] = None
        if names:
            options["names"] = names
    if "${" in path:
        context.report.warnings.append(f"'{step.name}': the file path '{path}' uses a variable; define it as a pipeline parameter")
    return _Converted([_node(step, context, "source.file", {"path": path, "options": options})])


def csv_input(step: Step, context: _Context) -> _Converted:
    if context.incoming.get(step.name) or step.text("filename_field"):
        raise Unsupported("file names that come from another step are not supported")
    names = [text_of(f, "name") for f in step.element.findall("./fields/field")]
    return _file_source(step, context, step.text("filename"), step.text("separator", ","), step.flag("header", True), names)


def text_file_input(step: Step, context: _Context) -> _Converted:
    files = step.element.findall("./file/name")
    if len(files) != 1 or step.flag("accept_filenames"):
        raise Unsupported("only one fixed file is supported")
    if step.text("file_type", "CSV").upper() == "FIXED" or step.flag("fixed"):
        raise Unsupported("fixed-width files are not supported")
    names = [text_of(f, "name") for f in step.element.findall("./fields/field")]
    return _file_source(step, context, (files[0].text or "").strip(), step.text("separator", ","), step.flag("header", True), names)


def excel_input(step: Step, context: _Context) -> _Converted:
    files = step.element.findall("./file/name")
    sheets = step.element.findall("./sheets/sheet")
    if len(files) != 1 or len(sheets) > 1:
        raise Unsupported("only one file and one sheet are supported")
    config: Dict[str, object] = {"path": (files[0].text or "").strip(), "options": {}}
    if sheets and text_of(sheets[0], "name"):
        config["options"] = {"sheet_name": text_of(sheets[0], "name")}
    if not step.flag("header", True):
        config["options"]["header"] = None
    if not config["path"]:
        raise Unsupported("no file name")
    return _Converted([_node(step, context, "source.file", config)])


def select_values(step: Step, context: _Context) -> _Converted:
    if step.element.findall("./fields/meta"):
        context.report.warnings.append(f"'{step.name}': data type changes (the Meta-data tab) are not applied")
    columns = []
    for field_ in step.element.findall("./fields/field"):
        name = text_of(field_, "name")
        if name:
            columns.append({"name": name, **({"rename": text_of(field_, "rename")} if text_of(field_, "rename") else {})})
    remove = [text_of(r, "name") for r in step.element.findall("./fields/remove") if text_of(r, "name")]
    config: Dict[str, object] = {}
    if columns:
        config["columns"] = columns
        if step.element.findtext("./fields/select_unspecified", "N").strip().upper() == "Y":
            config["keep_others"] = True
    if remove:
        config["remove"] = remove
    return _Converted([_node(step, context, "transform.select", config)])


def constant(step: Step, context: _Context) -> _Converted:
    fields = []
    for field_ in step.element.findall("./fields/field"):
        kind = text_of(field_, "type", "String")
        value = text_of(field_, "nullif")
        if kind not in ("String", "Integer", "Number"):
            raise Unsupported(f"constants of type '{kind}' are not supported")
        if kind != "String" and value:
            value = int(value) if kind == "Integer" and value.lstrip("+-").isdigit() else float(value)
        fields.append({"name": text_of(field_, "name"), "value": value})
    if not fields:
        raise Unsupported("no constants defined")
    return _Converted([_node(step, context, "transform.constant", {"fields": fields})])


def sort_rows(step: Step, context: _Context) -> _Converted:
    by = [{"column": text_of(f, "name"), "ascending": text_of(f, "ascending", "Y").upper() == "Y"}
          for f in step.element.findall("./fields/field") if text_of(f, "name")]
    if not by:
        raise Unsupported("no sort fields")
    return _Converted([_node(step, context, "transform.sort", {"by": by})])


def filter_rows(step: Step, context: _Context) -> _Converted:
    if step.text("send_true_to") and step.text("send_false_to"):
        raise Unsupported("a filter with separate outputs for true and false rows is not supported")
    if step.text("send_false_to") and not step.text("send_true_to"):
        raise Unsupported("a filter that only sends the rows that fail the condition is not supported")
    condition = step.element.find("./compare/condition")
    if condition is None:
        raise Unsupported("no condition")
    return _Converted([_node(step, context, "transform.filter", {"condition": _condition(condition)})])


def calculator(step: Step, context: _Context) -> _Converted:
    nodes: List[Node] = []
    computed: List[Dict[str, str]] = []
    for index, calculation in enumerate(step.element.findall("./calculation")):
        name, kind = text_of(calculation, "field_name"), text_of(calculation, "calc_type")
        a, b = text_of(calculation, "field_a"), text_of(calculation, "field_b")
        if kind == "CONSTANT":
            raise Unsupported("'Create a constant' calculations are not supported; use a Constant step")
        if kind not in CALCULATIONS or not name or not a or (kind != "COPY_OF_FIELD" and not b):
            raise Unsupported(f"the calculation '{kind or 'unknown'}' for '{name}' is not supported")
        computed.append({"name": name, "formula": CALCULATIONS[kind].format(a=_column(a), b=_column(b))})
        if text_of(calculation, "remove", "N").upper() == "Y":
            context.report.warnings.append(f"'{step.name}': the temporary field '{name}' is kept in the output")
    if not computed:
        raise Unsupported("no calculations")
    nodes.append(_node(step, context, "transform.compute", {"columns": computed}))
    return _Converted(nodes)


def dummy(step: Step, context: _Context) -> _Converted:
    return _Converted([_node(step, context, "transform.select", {})])


def merge_join(step: Step, context: _Context) -> _Converted:
    how = JOIN_TYPES.get(step.text("join_type", "INNER").upper())
    if how is None:
        raise Unsupported(f"the join type '{step.text('join_type')}' is not supported")
    keys_left = [text_of(k, ".") for k in step.element.findall("./keys_1/key")] or [text_of(k, ".") for k in step.element.findall("./keys1/key")]
    keys_right = [text_of(k, ".") for k in step.element.findall("./keys_2/key")] or [text_of(k, ".") for k in step.element.findall("./keys2/key")]
    if not keys_left or len(keys_left) != len(keys_right):
        raise Unsupported("the join keys are missing or do not match in number")
    return _Converted([_node(step, context, MERGE_JOIN, {"keys_left": keys_left, "keys_right": keys_right, "how": how})])


def table_output(step: Step, context: _Context) -> _Converted:
    connection = context.transformation.connections.get(step.text("connection"))
    if connection is None:
        raise Unsupported(f"the connection '{step.text('connection')}' is not defined in the file")
    if step.flag("tablename_in_field") or step.flag("partitioning_enabled"):
        raise Unsupported("table names from a field and partitioned tables are not supported")
    if not step.text("table"):
        raise Unsupported("no table name")
    config: Dict[str, object] = {"connection_string": connection_url(connection, context.report),
                                 "table_name": step.text("table"),
                                 "if_exists": "truncate" if step.flag("truncate") else "append"}
    if step.text("schema"):
        config["schema"] = step.text("schema")
    nodes: List[Node] = []
    if step.flag("specify_fields"):
        mapping = [(text_of(f, "stream_name"), text_of(f, "column_name")) for f in step.element.findall("./fields/field")]
        mapping = [(stream, column) for stream, column in mapping if stream and column]
        if mapping:  # the database column list also selects and renames the stream's fields
            nodes.append(_node(step, context, "transform.select",
                               {"columns": [{"name": s, **({"rename": c} if c != s else {})} for s, c in mapping]}, "__fields"))
    nodes.append(_node(step, context, "destination.database", config))
    return _Converted(nodes)


def text_file_output(step: Step, context: _Context) -> _Converted:
    name = text_of(step.element, "./file/name")
    if not name:
        raise Unsupported("no file name")
    extension = text_of(step.element, "./file/extention") or text_of(step.element, "./file/extension")
    path = f"{name}.{extension}" if extension and not name.endswith(f".{extension}") else name
    if step.flag("fileAsCommand") or step.flag("servlet_output"):
        raise Unsupported("output to a command or a servlet is not supported")
    options: Dict[str, object] = {}
    if step.text("separator") and step.text("separator") != ",":
        options["sep"] = step.text("separator")
    if step.text("encoding"):
        options["encoding"] = step.text("encoding")
    if not step.flag("header", True):
        options["header"] = False
    config: Dict[str, object] = {"path": path, "format": "csv"}
    if options:
        config["options"] = options
    return _Converted([_node(step, context, "destination.file", config)])


CONVERTERS: Dict[str, Callable[[Step, _Context], _Converted]] = {
    "TableInput": table_input, "CsvInput": csv_input, "TextFileInput": text_file_input, "ExcelInput": excel_input,
    "SelectValues": select_values, "Constant": constant, "SortRows": sort_rows, "FilterRows": filter_rows,
    "Calculator": calculator, "Dummy": dummy, "MergeJoin": merge_join,
    "TableOutput": table_output, "TextFileOutput": text_file_output,
}


def _unique_ids(steps: List[Step]) -> Dict[str, str]:
    ids: Dict[str, str] = {}
    taken: set = set()
    for step in steps:
        base, candidate, n = slug(step.name), slug(step.name), 2
        while candidate in taken or candidate.endswith("__fields"):
            candidate, n = f"{base}_{n}", n + 1
        taken.add(candidate)
        ids[step.name] = candidate
    return ids


def transformation_to_pipeline(transformation: Transformation) -> Tuple[Pipeline, ConversionReport]:
    report = ConversionReport()
    pipeline = Pipeline(name=transformation.name, description=transformation.description,
                        parameters=dict(transformation.parameters))
    ids = _unique_ids(transformation.steps)
    incoming: Dict[str, List[str]] = {}
    for hop in transformation.hops:
        if hop.enabled:
            incoming.setdefault(hop.to_step, []).append(hop.from_step)
    context = _Context(transformation, report, ids, incoming)

    entry: Dict[str, str] = {}
    exit_: Dict[str, str] = {}
    by_name = {s.name: s for s in transformation.steps}
    for step in transformation.steps:
        try:
            if step.type not in CONVERTERS:
                raise Unsupported(f"the step type '{step.type}' is not supported")
            converted = CONVERTERS[step.type](step, context)
            report.converted.append(step.name)
        except Unsupported as reason:
            report.unsupported.append({"step": step.name, "type": step.type, "reason": str(reason)})
            converted = _Converted([Node(id=ids[step.name], type=f"unsupported.{step.type or 'Unknown'}", label=step.name,
                                         config={**safe_config(step), "reason": str(reason)},
                                         position={"x": step.x, "y": step.y})])
        pipeline.nodes.extend(converted.nodes)
        for first, second in zip(converted.nodes, converted.nodes[1:]):
            pipeline.edges.append(Edge(first.id, second.id))
        entry[step.name], exit_[step.name] = converted.nodes[0].id, converted.nodes[-1].id

    for hop in transformation.hops:
        if not hop.enabled:
            report.warnings.append(f"The hop {hop.from_step} -> {hop.to_step} is disabled and was left out")
            continue
        if hop.from_step not in exit_ or hop.to_step not in entry:
            report.warnings.append(f"The hop {hop.from_step} -> {hop.to_step} refers to a missing step and was left out")
            continue
        handle = None
        target = by_name[hop.to_step]
        if target.type == "MergeJoin":
            handle = "left" if hop.from_step == target.text("step1") else "right" if hop.from_step == target.text("step2") else None
            if handle is None:
                report.warnings.append(f"'{target.name}': the input from '{hop.from_step}' is neither step1 nor step2")
        pipeline.edges.append(Edge(exit_[hop.from_step], entry[hop.to_step], handle))
    return pipeline, report


__all__ = ["transformation_to_pipeline", "ConversionReport", "connection_url", "slug", "Unsupported"]
