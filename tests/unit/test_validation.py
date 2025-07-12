import pandas as pd
from rdflib import Graph
from bocagrande.validation import validate_dataframe
from ontology.model import TableSchema, PropertyDef


class DummyReasoner:
    def __init__(self):
        self.called = False

    def reason(self, owl_path: str, *, loop=None, timeout=None):
        self.called = True
        return True, "ok"


def test_validate_dataframe_runs_reasoner_and_shacl(monkeypatch, tmp_path):
    schema = TableSchema("TEST", [PropertyDef("name")])
    df = pd.DataFrame({"name": ["a"]})

    def fake_validate(owl_path, shacl_path=None):
        assert owl_path
        assert shacl_path
        return True, "shacl ok"

    monkeypatch.setattr("bocagrande.validation.validate_shacl", fake_validate)

    reasoner = DummyReasoner()
    hermit_ok, shacl_ok, logs_hermit, logs_shacl = validate_dataframe(df, schema, reasoner=reasoner)
    assert hermit_ok is True
    assert shacl_ok is True
    assert reasoner.called
    assert "ok" in logs_hermit
    assert "shacl ok" in logs_shacl
