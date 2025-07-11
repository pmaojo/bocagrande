"""
Dominio: entidades para TBox (clases, propiedades) y ABox (individuos).
"""
from typing import List, Dict, Any

class PropertyDef:
    """Definición de una propiedad de datos OWL."""
    def __init__(self, name: str, tipo: str = "string", requerido: bool = False, metadata: Dict[str, Any] = None):
        self.name = name
        self.tipo = tipo
        self.requerido = requerido
        self.metadata = metadata or {}

class ClassDef:
    """Definición de una clase OWL (tabla YAML)."""
    def __init__(self, name: str, properties: List[PropertyDef]):
        self.name = name
        self.properties = properties

class Individual:
    """Instancia de una clase OWL (fila de datos)."""
    def __init__(self, class_name: str, values: Dict[str, Any]):
        self.class_name = class_name
        self.values = values

class TableSchema:
    """Esquema de tabla cargado desde YAML, con metadatos y campos."""
    def __init__(self, name: str, fields: List[PropertyDef], primary_key: List[str] = None, metadata: Dict[str, Any] = None):
        self.name = name
        self.fields = fields
        self.primary_key = primary_key or []
        self.metadata = metadata or {}
