import pandas as pd
import pytest

from adapter.decision import Decision, DecisionService
from adapter.shacl_runner import Violation
from bocagrande.shacl_triage import (
    DATA_ERROR, MAPPING_ERROR, NEEDS_REVIEW, SCHEMA_ERROR, ViolationGroup, group_violations, triage,
)
from bocagrande.validation import validate_dataframe_report
from ontology.model import PropertyDef, TableSchema


class FakeDecisions(DecisionService):
    def __init__(self, answers=None, error=None):
        self.answers, self.error, self.calls = answers or {}, error, []

    def decide(self, state, questions):
        self.calls.append(questions)
        if self.error:
            raise self.error
        out = {}
        for k in questions:
            answer, confidence = self.answers.get(k, (SCHEMA_ERROR, 0.95))
            out[k] = Decision("choice", answer, confidence=confidence)
        return out


def viol(path, comp, row, value=None):
    return Violation(f"http://x#R{row}", path, comp, value, "msg", "Violation")


def schema():
    return TableSchema(name="T", fields=[PropertyDef("nombre", requerido=True, length=5), PropertyDef("edad", "integer")])


def groups_for(violations, total=100):
    return group_violations(violations, schema(), total)


def test_grouping_counts_rows_not_violations_and_keeps_examples():
    vs = [viol("NOMBRE", "MaxLength", r, f"v{r % 3}") for r in range(10)] + [viol("EDAD", "Datatype", 1, "abc")]
    by_path = {g.path: g for g in groups_for(vs)}
    assert by_path["NOMBRE"].rows == 10 and by_path["NOMBRE"].field.name == "nombre"
    assert sorted(by_path["NOMBRE"].examples) == ["v0", "v1", "v2"]
    assert groups_for(vs)[0].path == "NOMBRE"  # largest first


def test_isolated_failures_are_data_errors_without_calling_the_model():
    fake = FakeDecisions()
    (result,) = triage(groups_for([viol("EDAD", "Datatype", 3, "abc")]), fake)
    assert result.category == DATA_ERROR and result.source == "rule" and result.confidence is None
    assert fake.calls == []
    assert "Fix or remove" in result.action


def test_a_required_field_empty_everywhere_is_a_mapping_error():
    vs = [viol("NOMBRE", "MinCount", r) for r in range(100)]
    (result,) = triage(groups_for(vs), FakeDecisions())
    assert result.category == MAPPING_ERROR and result.source == "rule"


def test_the_rest_go_to_the_model_in_one_request():
    vs = [viol("NOMBRE", "MaxLength", r, "Alejandra") for r in range(60)] + [viol("EDAD", "Datatype", r, "x") for r in range(30)]
    fake = FakeDecisions({"0": (SCHEMA_ERROR, 0.91), "1": (DATA_ERROR, 0.88)})
    results = triage(groups_for(vs), fake)
    assert len(fake.calls) == 1 and len(fake.calls[0]) == 2
    assert [(r.category, r.source) for r in results] == [(SCHEMA_ERROR, "decision"), (DATA_ERROR, "decision")]
    assert results[0].confidence == pytest.approx(0.91)
    assert "Review the MaxLength constraint" in results[0].action


def test_low_confidence_and_invalid_answers_need_review():
    vs = [viol("NOMBRE", "MaxLength", r) for r in range(50)] + [viol("EDAD", "Datatype", r) for r in range(50)]
    fake = FakeDecisions({"0": (SCHEMA_ERROR, 0.5), "1": ("something_else", 0.99)})
    results = triage(groups_for(vs), fake)
    assert [r.category for r in results] == [NEEDS_REVIEW, NEEDS_REVIEW]
    assert all(r.source == "escalated" for r in results)


def test_without_a_model_or_when_it_fails_groups_need_review():
    vs = [viol("NOMBRE", "MaxLength", r) for r in range(50)]
    assert triage(groups_for(vs), None)[0].source == "no_model"
    failing = triage(groups_for(vs), FakeDecisions(error=RuntimeError("down")))
    assert failing[0].category == NEEDS_REVIEW and failing[0].source == "no_model"


def test_example_values_are_sent_only_when_requested():
    vs = [viol("NOMBRE", "MaxLength", r, "SECRET-NAME") for r in range(50)]
    quiet, loud = FakeDecisions(), FakeDecisions()
    triage(groups_for(vs), quiet)
    triage(groups_for(vs), loud, send_samples=True)
    assert "SECRET-NAME" not in str(quiet.calls[0]["0"].instructions)
    assert "SECRET-NAME" in str(loud.calls[0]["0"].instructions)


def test_the_question_describes_the_field_and_the_numbers():
    vs = [viol("NOMBRE", "MaxLength", r) for r in range(50)]
    fake = FakeDecisions()
    triage(groups_for(vs), fake)
    info = fake.calls[0]["0"].instructions
    assert info["violation"] == {"constraint": "MaxLength", "rows_affected": 50, "total_rows": 100, "share_of_rows": 0.5}
    assert info["field"]["name"] == "nombre" and info["field"]["length"] == 5


def test_end_to_end_report_with_real_validation():
    fields = [PropertyDef("id", "integer", requerido=True), PropertyDef("nombre", requerido=True, length=5),
              PropertyDef("edad", "integer")]
    sch = TableSchema(name="CLIENTES", fields=fields)
    n = 100
    df = pd.DataFrame({"id": list(range(n)), "nombre": ["Alejandra"] * n, "edad": pd.Series([30] * n, dtype=object)})
    df.loc[7, "edad"] = "abc"
    fake = FakeDecisions({"0": (SCHEMA_ERROR, 0.93)})

    report = validate_dataframe_report(df, sch, reasoner=None, decisions=fake)

    assert report.shacl_ok is False and report.reasoner_ok is True
    by_field = {t.group.field_name: t for t in report.triage}
    assert by_field["edad"].category == DATA_ERROR and by_field["edad"].source == "rule"
    assert by_field["nombre"].category == SCHEMA_ERROR and by_field["nombre"].source == "decision"


def test_a_conforming_dataset_has_nothing_to_triage():
    sch = TableSchema(name="T", fields=[PropertyDef("a", "integer")])
    report = validate_dataframe_report(pd.DataFrame({"a": [1, 2]}), sch, reasoner=None)
    assert report.shacl_ok and report.triage == []
