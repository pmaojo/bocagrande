import os
from unittest.mock import Mock

import pandas as pd
from rdflib import Graph

from ontology.service import OntologyBuilder
from ontology.model import TableSchema, PropertyDef

class DummyReasoner:
    def __init__(self):
        self.called_with = None
    def reason(self, owl_path: str):
        self.called_with = owl_path
        return True, "ok"


def test_reason_graph_calls_reasoner(tmp_path):
    reasoner = DummyReasoner()
    builder = OntologyBuilder(Graph(), reasoner=reasoner)
    schema = TableSchema("TEST", [PropertyDef("name")])
    df = pd.DataFrame({"name": ["a"]})
    g = builder.build_abox_graph(schema, df)
    ok, logs = builder.reason_graph(g)
    assert ok is True
    assert logs == "ok"
    assert reasoner.called_with and os.path.exists(reasoner.called_with)

