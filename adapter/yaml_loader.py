"""Infrastructure helpers to load table schemas from YAML files."""

from ontology.model import TableSchema, PropertyDef
import yaml
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import re


def _parse_length(value: Any) -> Optional[int]:
    """Return an int length if ``value`` is purely numeric, else ``None``."""
    if value is None:
        return None
    try:
        if isinstance(value, int):
            return value
        text = str(value).strip()
        return int(text) if text.isdigit() else None
    except (TypeError, ValueError):
        return None


def _parse_precision_scale(value: Any) -> Tuple[Optional[int], Optional[int]]:
    """Return ``(precision, scale)`` parsed from ``value`` if it matches
    ``"<int>,<int>"`` pattern."""
    if value is None:
        return None, None
    text = str(value).strip()
    if re.match(r"^\d+,\s*\d+$", text):
        p, s = text.split(",", 1)
        try:
            return int(p.strip()), int(s.strip())
        except ValueError:
            return None, None
    return None, None


def _is_required(value: Any) -> bool:
    """Return ``True`` if ``value`` means the field is required."""
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"sí", "si", "yes", "true", "1"}

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
        else:
            table_name = data.get('table') or Path(yaml_path).stem.upper()
            if any(k in data for k in ('columns', 'fields', 'campos')):
                table_def = data
    elif isinstance(data, list) and all(isinstance(item, dict) and 'Campo' in item for item in data):
        # Caso: El archivo es una lista directa de definiciones de campo (como CLIENTES.yaml)
        table_name = Path(yaml_path).stem.upper() # Usar el nombre del archivo como nombre de la tabla
        table_def = {'fields': data} # Envolver la lista en un diccionario con la clave 'fields'
    else:
        return None # Formato no reconocido

    if table_name is None or table_def is None:
        return None

    fields: List[PropertyDef] = []
    campos = (
        table_def.get('columns')
        or table_def.get('fields')
        or table_def.get('campos')
        or []
    )
    for col in campos:
        if isinstance(col, dict):
            field_name = col.get('name') or col.get('Campo') or ''
            length = _parse_length(col.get('length') or col.get('Longitud'))
            prec, scale = _parse_precision_scale(col.get('length') or col.get('Longitud'))
            if 'precision' in col or 'scale' in col:
                prec = col.get('precision', prec)
                scale = col.get('scale', scale)
            requerido = _is_required(col.get('required') or col.get('Obligatorio'))
            formato = col.get('format') or col.get('Formato')

            metadata_keys = set(col.keys()) - {
                'Campo',
                'name',
                'Tipo',
                'type',
                'Obligatorio',
                'required',
                'Longitud',
                'length',
                'precision',
                'scale',
                'enum',
                'foreign_key',
                'deprecated',
                'Formato',
                'format',
            }
            metadata: Dict[str, Any] = {k: col[k] for k in metadata_keys}
            if prec is not None:
                metadata['precision'] = prec
            if scale is not None:
                metadata['scale'] = scale
            if 'enum' in col:
                metadata['enum'] = col['enum']
            if 'foreign_key' in col:
                metadata['foreign_key'] = col['foreign_key']
            if 'deprecated' in col:
                metadata['deprecated'] = col['deprecated']
            fields.append(
                PropertyDef(
                    name=field_name,
                    tipo=col.get('type') or col.get('Tipo', 'string'),
                    requerido=requerido,
                    length=length,
                    formato=formato,
                    metadata=metadata,
                )
            )
        elif isinstance(col, str):
            fields.append(PropertyDef(name=col))
            
    primary_key = table_def.get('primary_key', [])
    unique = table_def.get('unique', [])
    metadata = {
        k: v
        for k, v in table_def.items()
        if k
        not in {
            'fields',
            'columns',
            'campos',
            'primary_key',
            'unique',
            'table',
            'description',
            'checks',
            'notes',
        }
    }
    return TableSchema(
        name=table_name,
        fields=fields,
        primary_key=primary_key,
        unique=unique,
        metadata=metadata,
    )
