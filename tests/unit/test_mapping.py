import pandas as pd
import pytest

from adapter.decision import Decision, DecisionService
from bocagrande.decision_gate import DecisionGate
from bocagrande.mapping import NONE_OPTION, normalize, suggest_steps
from bocagrande.transform import ETLStep
from ontology.model import PropertyDef, TableSchema


class FakeDecisions(DecisionService):
    """answers maps a field name to (header, confidence); unknown fields answer NONE."""

    def __init__(self, answers=None, error=None):
        self.answers, self.error, self.calls = answers or {}, error, []

    def decide(self, state, questions):
        self.calls.append((state, questions))
        if self.error:
            raise self.error
        out = {}
        for name in questions:
            header, conf = self.answers.get(name, (NONE_OPTION, 0.95))
            out[name] = Decision("choice", header, confidence=conf)
        return out


def schema(*names):
    return TableSchema(name="T", fields=[PropertyDef(name=n) for n in names])


def test_normalize_ignores_case_accents_and_separators():
    assert normalize("Fecha_Nacimiento") == normalize("fecha nacimiento") == normalize("fechaNacimiénto")


def test_names_are_mapped_without_calling_the_model():
    fake = FakeDecisions()
    result = suggest_steps(["Nombre", "fecha_nac"], schema("nombre", "fechaNac"), fake)
    assert [(s.campo_salida, s.campo_entrada) for s in result.steps] == [("nombre", "Nombre"), ("fechaNac", "fecha_nac")]
    assert set(result.sources.values()) == {"name"} and result.pending == []
    assert fake.calls == []


def test_confident_decisions_fill_the_remaining_fields():
    fake = FakeDecisions({"telefono": ("tel_movil", 0.93)})
    result = suggest_steps(["nombre", "tel_movil"], schema("nombre", "telefono"), fake)
    assert result.sources == {"nombre": "name", "telefono": "decision"}
    assert result.confidence["telefono"] == pytest.approx(0.93) and result.pending == []
    assert len(fake.calls) == 1  # one request for all remaining fields


def test_low_confidence_and_none_stay_pending():
    fake = FakeDecisions({"telefono": ("tel_movil", 0.5)})
    result = suggest_steps(["nombre", "tel_movil", "x"], schema("nombre", "telefono", "ciudad"), fake)
    assert result.pending == ["telefono", "ciudad"]
    assert [s.campo_salida for s in result.steps] == ["nombre"]


def test_a_column_feeds_only_one_field_the_most_confident_wins():
    fake = FakeDecisions({"a": ("col", 0.85), "b": ("col", 0.95)})
    result = suggest_steps(["col"], schema("a", "b"), fake)
    assert [(s.campo_salida, s.campo_entrada) for s in result.steps] == [("b", "col")]
    assert result.pending == ["a"]


def test_an_answer_outside_the_offered_columns_is_ignored():
    fake = FakeDecisions({"a": ("invented", 0.99)})
    assert suggest_steps(["col"], schema("a"), fake).pending == ["a"]


def test_model_failure_leaves_fields_pending_instead_of_raising():
    fake = FakeDecisions(error=RuntimeError("down"))
    result = suggest_steps(["col"], schema("a"), fake)
    assert result.pending == ["a"] and result.steps == []


def test_fallback_only_fills_pending_fields():
    def fallback(headers, sch):
        return [ETLStep("nombre", "WRONG"), ETLStep("telefono", "tel_movil")]

    result = suggest_steps(["nombre", "tel_movil"], schema("nombre", "telefono"), None, fallback=fallback)
    by_field = {s.campo_salida: s.campo_entrada for s in result.steps}
    assert by_field == {"nombre": "nombre", "telefono": "tel_movil"}
    assert result.sources == {"nombre": "name", "telefono": "fallback"}


def test_a_failing_fallback_is_survivable():
    def fallback(headers, sch):
        raise RuntimeError("llm down")

    assert suggest_steps(["x"], schema("a"), None, fallback=fallback).pending == ["a"]


def test_sample_values_are_not_sent_unless_requested():
    sample = pd.DataFrame({"tel": ["600111222", "600333444"]})
    quiet, loud = FakeDecisions(), FakeDecisions()
    suggest_steps(["tel"], schema("telefono"), quiet, sample=sample)
    suggest_steps(["tel"], schema("telefono"), loud, sample=sample, send_samples=True)
    assert "600111222" not in str(quiet.calls[0][1]["telefono"].criteria)
    assert "600111222" in str(loud.calls[0][1]["telefono"].criteria)


def test_gate_requires_confidence_and_a_valid_option():
    gate = DecisionGate(0.8)
    assert gate.accepts(Decision("choice", "a", confidence=0.9), allowed=["a", "b"])
    assert not gate.accepts(Decision("choice", "a", confidence=0.7), allowed=["a"])
    assert not gate.accepts(Decision("choice", "z", confidence=0.99), allowed=["a"])
