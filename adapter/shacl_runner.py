"""
Infraestructura: validación SHACL con pyshacl.
"""
from typing import Tuple
from pyshacl import validate

def validate_shacl(owl_path: str, shacl_path: str = None) -> Tuple[bool, str]:
    """
    Valida un OWL con pyshacl y devuelve (valido, logs).
    Si no se provee shacl_path, usa SHACL Core Shapes.
    """
    try:
        result = validate(
            data_graph=owl_path,
            shacl_graph=shacl_path,
            inference='rdfs',
            serialize_report_graph=True,
            debug=False
        )
        conforms, report_graph, report_text = result
        return bool(conforms), report_text
    except Exception as e:
        return False, f"Error en validación SHACL: {str(e)}"
