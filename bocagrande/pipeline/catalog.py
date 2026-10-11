"""What each node type needs, in a form an editor can turn into a form.

Field ``kind`` is one of: text, number, bool, select (with ``options``) or json (a list or object written as JSON).
"""
from __future__ import annotations

from typing import Any, Dict, List

FILE_FORMATS = ["", "csv", "xlsx", "parquet", "json"]


def _field(name: str, kind: str = "text", required: bool = False, help: str = "", **extra: Any) -> Dict[str, Any]:
    return {"name": name, "kind": kind, "required": required, "help": help, **extra}


CATALOG: Dict[str, Dict[str, Any]] = {
    "source.file": {"label": "Read file", "fields": [
        _field("path", required=True, help="CSV, Excel or Parquet file, relative to the working directory"),
        _field("options", "json", help='Reader options, e.g. {"sep": ";"}'),
        _field("text_columns", "json", help="Columns to keep as text, e.g. [\"id\"]")]},
    "source.database": {"label": "Read database", "fields": [
        _field("connection_string", required=True, help="SQLAlchemy URL; use ${NAME} for secrets"),
        _field("query", help="SQL query (or give a table)"), _field("table_name", help="Table to read")]},
    "transform.select": {"label": "Select / rename", "fields": [
        _field("columns", "json", help='[{"name": "id", "rename": "client_id"}]'),
        _field("remove", "json", help='["column"]'), _field("keep_others", "bool")]},
    "transform.mapping": {"label": "Mapping steps", "fields": [_field("steps", "json", True, "List of mapping steps")]},
    "transform.compute": {"label": "Compute columns", "fields": [
        _field("columns", "json", True, '[{"name": "total", "formula": "price * qty"}]')]},
    "transform.constant": {"label": "Constant columns", "fields": [
        _field("fields", "json", True, '[{"name": "source", "value": "web"}]')]},
    "transform.sort": {"label": "Sort", "fields": [
        _field("by", "json", True, '[{"column": "age", "ascending": false}]')]},
    "transform.filter": {"label": "Filter rows", "fields": [_field("condition", required=True, help="e.g. age >= 18")]},
    "transform.merge_join": {"label": "Merge join", "inputs": ["left", "right"], "fields": [
        _field("keys_left", "json", True, '["id"]'), _field("keys_right", "json", True, '["id"]'),
        _field("how", "select", options=["inner", "left", "right", "outer"])]},
    "destination.database": {"label": "Write database", "fields": [
        _field("connection_string", required=True), _field("table_name", required=True),
        _field("if_exists", "select", options=["fail", "replace", "append", "truncate"])]},
    "destination.file": {"label": "Write file", "fields": [
        _field("path", required=True), _field("format", "select", options=FILE_FORMATS),
        _field("options", "json", help='CSV options: sep, encoding, header, quotechar')]},
    "destination.graph": {"label": "Knowledge graph", "fields": [
        _field("schema", required=True, help="YAML schema file"), _field("output", required=True, help="Turtle file to write"),
        _field("reasoner", "select", options=["ontologos", "hermit", "none"]),
        _field("validate", "bool"), _field("fail_on_violations", "bool")]},
}


def catalog() -> List[Dict[str, Any]]:
    return [{"type": type_, **entry} for type_, entry in CATALOG.items()]
