"""Domain entities for TBox (classes, properties) and ABox (individuals)."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class PropertyDef:
    """Data property definition for OWL."""

    name: str
    tipo: str = "string"
    requerido: bool = False
    length: Optional[int] = None
    formato: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ClassDef:
    """OWL class definition (YAML table)."""

    name: str
    properties: list[PropertyDef]

@dataclass
class Individual:
    """Instance of an OWL class (row of data)."""

    class_name: str
    values: Dict[str, Any]

@dataclass
class TableSchema:
    """Table schema loaded from YAML with metadata and fields."""

    name: str
    fields: list[PropertyDef]
    primary_key: list[str] = field(default_factory=list)
    unique: list[list[str]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def all_unique_cols(self) -> set[str]:
        """Return the union of all unique column names."""
        cols: set[str] = set()
        for group in self.unique:
            cols.update(group)
        return cols
