"""Infrastructure: loader for the master data contract YAML."""
from dataclasses import dataclass, field
from typing import Any, Dict, List

import yaml

from .yaml_loader import load_schema
from ontology.model import TableSchema


@dataclass
class TableEntry:
    """Table definition inside a data contract."""

    file: str
    schema: TableSchema
    description: str | None = None
    notes: List[str] = field(default_factory=list)


@dataclass
class DataContract:
    """Structured representation of a data contract."""

    name: str
    version: str
    owner: str
    globals: Dict[str, Any]
    tables: List[TableEntry]


def load_data_contract(path: str) -> DataContract:
    """Load a master data contract from ``path``."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    info = data.get("contract", {})
    globals_cfg = data.get("globals", {})

    tables: List[TableEntry] = []
    for entry in data.get("tables", []):
        schema_path = entry["schema"]
        schema = load_schema(schema_path)
        if schema is None:
            continue
        if pk := entry.get("primary_key"):
            schema.primary_key = pk
        if unique := entry.get("unique"):
            schema.unique = unique
        if desc := entry.get("description"):
            schema.metadata["description"] = desc
        if notes := entry.get("notes"):
            schema.metadata["notes"] = notes
        tables.append(TableEntry(file=entry["file"], schema=schema, description=desc, notes=notes or []))

    return DataContract(
        name=info.get("name", ""),
        version=info.get("version", ""),
        owner=info.get("owner", ""),
        globals=globals_cfg,
        tables=tables,
    )
