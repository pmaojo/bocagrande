"""
Infraestructura: carga de esquemas YAML a TableSchema.
"""
from ontology.model import TableSchema, PropertyDef
import yaml
from typing import List, Dict, Any, Optional
from pathlib import Path

def load_schema(yaml_path: str) -> Optional[TableSchema]:
    """
    Carga un archivo YAML y devuelve un TableSchema, o None si el formato no es válido.
    Soporta tanto lista de strings como de dicts para los campos.
    """
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    table_name = None
    table_def = None

    if isinstance(data, dict):
        if len(data) == 1 and list(data.keys())[0].isupper() and isinstance(list(data.values())[0], dict):
            # Caso: {T_CLIENTES: {...}}
            table_name, table_def = list(data.items())[0]
        elif 'fields' in data or 'columns' in data or 'campos' in data:
            # Caso: El propio archivo es la definición de la tabla (sin clave superior)
            table_name = Path(yaml_path).stem.upper() # Usar el nombre del archivo como nombre de la tabla
            table_def = data
    elif isinstance(data, list) and all(isinstance(item, dict) and 'Campo' in item for item in data):
        # Caso: El archivo es una lista directa de definiciones de campo (como CLIENTES.yaml)
        table_name = Path(yaml_path).stem.upper() # Usar el nombre del archivo como nombre de la tabla
        table_def = {'fields': data} # Envolver la lista en un diccionario con la clave 'fields'
    else:
        return None # Formato no reconocido

    if table_name is None or table_def is None:
        return None

    fields = []
    campos = table_def.get('fields') or table_def.get('columns') or table_def.get('campos') or []
    for col in campos:
        if isinstance(col, dict):
            field_name = col.get('Campo') or col.get('name') or '' # Priorizar 'Campo'
            fields.append(PropertyDef(
                name=field_name,
                tipo=col.get('Tipo', 'string'), # Actualizado a 'Tipo'
                requerido=col.get('Obligatorio', False) == 'Sí', # Actualizado a 'Obligatorio' y convertida a bool
                metadata={k: v for k, v in col.items() if k not in ['Campo', 'name', 'Tipo', 'Obligatorio', 'tipo', 'requerido']} # Actualizar exclusiones
            ))
        elif isinstance(col, str):
            fields.append(PropertyDef(name=col))
            
    primary_key = table_def.get('primary_key', [])
    metadata = {k: v for k, v in table_def.items() if k not in ['fields', 'columns', 'campos', 'primary_key']}
    return TableSchema(name=table_name, fields=fields, primary_key=primary_key, metadata=metadata)
