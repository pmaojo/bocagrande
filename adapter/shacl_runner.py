"""
Infraestructura: validación SHACL con pyshacl.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from pyshacl import validate
from rdflib import Namespace, RDF

SH = Namespace("http://www.w3.org/ns/shacl#")


@dataclass
class Violation:
    """One ``sh:ValidationResult``."""

    focus_node: str
    path: str  # local name of the property, as written in the graph (e.g. NOMBRE)
    component: str  # constraint kind without the suffix (Datatype, MinCount, MaxLength, Pattern...)
    value: Optional[str]
    message: str
    severity: str


@dataclass
class ShaclReport:
    conforms: bool
    text: str
    violations: List[Violation] = field(default_factory=list)


def _local(term) -> str:
    return str(term).rsplit("#", 1)[-1].rsplit("/", 1)[-1] if term is not None else ""

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


def validate_shacl_report(owl_path: str, shacl_path: str = None) -> ShaclReport:
    """Like :func:`validate_shacl` but also returns each violation as structured data."""
    try:
        conforms, report_graph, report_text = validate(
            data_graph=owl_path,
            shacl_graph=shacl_path,
            inference='rdfs',
            debug=False,
        )
    except Exception as e:
        return ShaclReport(False, f"Error en validación SHACL: {str(e)}")

    violations: List[Violation] = []
    for result in report_graph.subjects(RDF.type, SH.ValidationResult):
        value = report_graph.value(result, SH.value)
        violations.append(Violation(
            focus_node=str(report_graph.value(result, SH.focusNode)),
            path=_local(report_graph.value(result, SH.resultPath)),
            component=_local(report_graph.value(result, SH.sourceConstraintComponent)).replace("ConstraintComponent", ""),
            value=str(value) if value is not None else None,
            message=str(report_graph.value(result, SH.resultMessage) or ""),
            severity=_local(report_graph.value(result, SH.resultSeverity)),
        ))
    return ShaclReport(bool(conforms), report_text, violations)
