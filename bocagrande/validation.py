"""High level semantic validation helpers."""
from __future__ import annotations

from typing import Tuple
import tempfile
import os
from rdflib import Graph
import pandas as pd

from adapter.reasoner import Reasoner
from adapter.shacl_runner import validate_shacl
from adapter.yaml_to_shacl import generar_shape_shacl
from ontology.tbox_builder import build_global_tbox
from ontology.service import OntologyBuilder
from ontology.model import TableSchema


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

__all__ = ["validate_dataframe"]
