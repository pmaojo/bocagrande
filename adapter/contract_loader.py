"""Infrastructure: loader for the master data contract YAML."""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import yaml

from .yaml_loader import load_schema
from ontology.model import TableSchema


@dataclass
class TableEntry:
    """Table definition inside a data contract."""

    file: str
    schema: TableSchema
    description: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class DataContract:
    """Structured representation of a data contract."""

    name: str
    version: str
    owner: str
    globals: Dict[str, Any]
    tables: list[TableEntry]


__all__ = ["TableEntry", "DataContract", "load_data_contract"]


def load_data_contract(path: str | Path) -> DataContract:
    """Load a master data contract from ``path``."""
    path = Path(path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))

    info = data.get("contract", {})
    globals_cfg = data.get("globals", {})

    tables: list[TableEntry] = []
    for entry in data.get("tables", []):
        schema_path = Path(entry["schema"])
        schema = load_schema(schema_path)
        if schema is None:
            raise FileNotFoundError(f"Schema not found or invalid: {schema_path}")

        pk = entry.get("primary_key") or []
        unique = entry.get("unique") or []
        if isinstance(unique, list) and all(isinstance(u, str) for u in unique):
            unique = [unique]
        elif not (
            isinstance(unique, list)
            and all(isinstance(u, list) and all(isinstance(c, str) for c in u) for u in unique)
        ):
            raise TypeError("unique must be a list of string lists")

        schema.primary_key = pk
        schema.unique = unique

        desc = entry.get("description")
        notes = entry.get("notes", [])

        tables.append(
            TableEntry(file=entry["file"], schema=schema, description=desc, notes=notes)
        )

    return DataContract(
        name=info.get("name", ""),
        version=info.get("version", ""),
        owner=info.get("owner", ""),
        globals=globals_cfg,
        tables=tables,
    )
