"""Column dtypes to use when reading a file, so text columns keep their exact value.

pandas infers types when it reads a CSV: ``001`` becomes ``1`` and a postal code ``01234`` loses its
zero before the value ever reaches the graph. Columns the schema declares as ``string`` are read as text.
"""
from __future__ import annotations

from typing import Dict, Iterable, Optional

from bocagrande.transform import ETLStep
from ontology.model import TableSchema


def string_dtypes(schema: TableSchema, steps: Optional[Iterable[ETLStep]] = None) -> Dict[str, type]:
    """Return ``{column: str}`` for the input columns that feed ``string`` fields of ``schema``.

    Without ``steps`` the input columns are assumed to be named like the schema fields. With ``steps``
    (mappings from an input column to an output field) the input column of each mapping is used.
    """
    string_fields = {f.name for f in schema.fields if f.tipo == "string"}
    if steps is None:
        return {name: str for name in string_fields}
    return {
        step.campo_entrada: str
        for step in steps
        if step.campo_entrada
        and step.campo_salida in string_fields
        and step.tipo_transformacion in {"mapping", "map"}
    }


__all__ = ["string_dtypes"]
