"""High level semantic validation helpers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import tempfile
import os
from rdflib import Graph
import pandas as pd

from adapter.reasoner import Reasoner
from adapter.decision import DecisionService
from adapter.shacl_runner import ShaclReport, validate_shacl, validate_shacl_report
from adapter.yaml_to_shacl import generar_shape_shacl
from ontology.tbox_builder import build_global_tbox
from ontology.service import OntologyBuilder
from ontology.model import TableSchema
from bocagrande.shacl_triage import Triage, group_violations, triage


def validate_dataframe(
    df: pd.DataFrame,
    schema: TableSchema,
    *,
    reasoner: Reasoner,
) -> Tuple[bool, bool, str, str]:
    """Validate ``df`` against ``schema`` using the provided reasoner.

    Returns a tuple ``(hermit_ok, shacl_ok, hermit_logs, shacl_logs)``.
    """
    tbox = build_global_tbox()
    builder = OntologyBuilder(tbox, reasoner=reasoner)
    graph: Graph = builder.build_abox_graph(schema, df)

    with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False) as tmp:
        graph.serialize(destination=tmp.name, format="turtle")
        owl_path = tmp.name

    hermit_ok, hermit_logs = reasoner.reason(owl_path)

    shape_ttl = generar_shape_shacl(schema)
    with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False, mode="w") as tmp_sh:
        tmp_sh.write(shape_ttl)
        shacl_path = tmp_sh.name

    try:
        shacl_ok, shacl_logs = validate_shacl(owl_path, shacl_path)
    finally:
        for path in (owl_path, shacl_path):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    return hermit_ok, shacl_ok, hermit_logs, shacl_logs

@dataclass
class ValidationReport:
    reasoner_ok: bool
    reasoner_logs: str
    shacl: ShaclReport
    triage: List[Triage] = field(default_factory=list)

    @property
    def shacl_ok(self) -> bool:
        return self.shacl.conforms


def validate_dataframe_report(
    df: pd.DataFrame,
    schema: TableSchema,
    *,
    reasoner: Optional[Reasoner],
    decisions: Optional[DecisionService] = None,
    send_samples: bool = False,
    min_confidence: float = 0.8,
) -> ValidationReport:
    """Validate ``df`` and classify any SHACL violations by likely cause.

    Unlike :func:`validate_dataframe` this returns the violations as data and a triage per
    group of violations. ``reasoner`` may be ``None`` to skip reasoning.
    """
    tbox = build_global_tbox()
    builder = OntologyBuilder(tbox, reasoner=reasoner)
    graph: Graph = builder.build_abox_graph(schema, df)

    with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False) as tmp:
        graph.serialize(destination=tmp.name, format="turtle")
        owl_path = tmp.name
    with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False, mode="w") as tmp_sh:
        tmp_sh.write(generar_shape_shacl(schema))
        shacl_path = tmp_sh.name

    try:
        reasoner_ok, reasoner_logs = reasoner.reason(owl_path) if reasoner is not None else (True, "Reasoner skipped")
        shacl = validate_shacl_report(owl_path, shacl_path)
    finally:
        for path in (owl_path, shacl_path):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    groups = group_violations(shacl.violations, schema, total_rows=len(df))
    return ValidationReport(
        reasoner_ok, reasoner_logs, shacl,
        triage(groups, decisions, send_samples=send_samples, min_confidence=min_confidence),
    )


__all__ = ["validate_dataframe", "validate_dataframe_report", "ValidationReport"]
