"""Domain entities for TBox (classes, properties) and ABox (individuals)."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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
    properties: List[PropertyDef]

@dataclass
class Individual:
    """Instance of an OWL class (row of data)."""

    class_name: str
    values: Dict[str, Any]

@dataclass
class TableSchema:
    """Table schema loaded from YAML with metadata and fields."""

    name: str
    fields: List[PropertyDef]
    primary_key: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
