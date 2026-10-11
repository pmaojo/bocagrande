import pandas as pd
import pytest

from adapter.data_io import read_dataframe
from adapter.decision import Decision, DecisionService
from adapter.shacl_runner import validate_shacl_report
from adapter.yaml_loader import load_schema
from adapter.yaml_to_shacl import generar_shape_shacl
from bocagrande import schema_cli
from bocagrande.schema_inference import infer_schema, schema_to_yaml
from ontology.service import OntologyBuilder
from ontology.tbox_builder import build_global_tbox


def infer(columns, **kwargs):
    df = pd.DataFrame(columns)
    return infer_schema(df, "T", **kwargs).columns


class FakeDecisions(DecisionService):
    def __init__(self, answer="identifier", confidence=0.95, error=None):
        self.answer, self.confidence, self.error, self.calls = answer, confidence, error, []

    def decide(self, state, questions):
        self.calls.append((state, questions))
        if self.error:
            raise self.error
        return {k: Decision("choice", self.answer, confidence=self.confidence) for k in questions}


def test_basic_types_come_from_the_data():
    cols = infer({
        "n": ["1", "22", "333"], "x": ["1.5", "2", "3.25"], "b": ["true", "false", "true"],
        "t": ["ana", "luis", "eva"], "empty": [None, None, None],
    })
    assert [(c.tipo, c.source) for c in (cols["n"], cols["x"], cols["b"], cols["t"])] == [
        ("integer", "rule"), ("float", "rule"), ("boolean", "rule"), ("string", "default")]
    assert cols["empty"].tipo == "string" and cols["empty"].note == "no values"


def test_leading_zeros_and_very_long_numbers_are_identifiers():
    cols = infer({"shop": ["001", "002"], "iban": ["1234567890123456789", "9876543210987654321"]})
    assert cols["shop"].tipo == "string" and "leading zeros" in cols["shop"].note
    assert cols["iban"].tipo == "string"


def test_nulls_and_blanks_do_not_change_the_type():
    assert infer({"n": ["1", None, " ", "3"]})["n"].tipo == "integer"


def test_unambiguous_dates_are_found_by_rule():
    cols = infer({
        "iso": ["2025-07-12", "2024-01-31"],
        "dmy": ["12/07/2025", "31/01/2024"],        # 31 can only be a day
        "mdy": ["07/12/2025", "01/31/2024"],        # 31 can only be a day, in the second position
        "ts": ["2025-07-12T10:20:30Z", "2024-01-31T08:00:00Z"],
    })
    assert [(cols[c].tipo, cols[c].formato, cols[c].source) for c in ("iso", "dmy", "mdy", "ts")] == [
        ("date", "%Y-%m-%d", "rule"), ("date", "%d/%m/%Y", "rule"),
        ("date", "%m/%d/%Y", "rule"), ("datetime", "%Y-%m-%dT%H:%M:%SZ", "rule")]


def test_day_month_ambiguity_is_assumed_and_flagged_not_asked_to_a_model():
    values = {"d": ["01/02/2025", "03/04/2025"]}
    fake = FakeDecisions()
    day_first = infer(values, decisions=fake)["d"]
    month_first = infer(values, prefer_day_first=False)["d"]
    assert (day_first.formato, day_first.source) == ("%d/%m/%Y", "assumed")
    assert month_first.formato == "%m/%d/%Y" and "guess" in month_first.note
    assert fake.calls == []  # the data holds no information a model could add


def test_mixed_date_formats_below_the_match_threshold_stay_text():
    assert infer({"d": ["2025-07-12", "12/07/2025", "yesterday", "n/a"]})["d"].tipo == "string"


def test_fixed_length_digits_are_assumed_integer_without_a_model():
    col = infer({"cp": ["41001", "28013", "28001"]})["cp"]
    assert (col.tipo, col.source) == ("integer", "assumed") and "identifier" in col.note


def test_a_model_can_settle_identifier_versus_quantity_without_seeing_values():
    fake = FakeDecisions("identifier", 0.93)
    df = pd.DataFrame({"codigo_postal": ["41001", "28013"], "cantidad": ["1", "250"]})
    cols = infer_schema(df, "T", fake).columns
    assert (cols["codigo_postal"].tipo, cols["codigo_postal"].source) == ("string", "decision")
    assert cols["codigo_postal"].confidence == pytest.approx(0.93)
    assert cols["cantidad"].tipo == "integer"
    (_, questions), = fake.calls
    assert list(questions) == ["codigo_postal"]            # only the ambiguous column is asked
    sent = str(questions["codigo_postal"].instructions)
    assert "digits_max" in sent
    assert not any(v in sent for v in ("41001", "28013"))   # structure only: no value, minimum or maximum


def test_low_confidence_or_failing_model_keeps_the_assumed_type():
    df = pd.DataFrame({"cp": ["41001", "28013"]})
    unsure = infer_schema(df, "T", FakeDecisions("identifier", 0.4)).columns["cp"]
    broken = infer_schema(df, "T", FakeDecisions(error=RuntimeError("down"))).columns["cp"]
    assert (unsure.tipo, unsure.source) == ("integer", "assumed")
    assert (broken.tipo, broken.source) == ("integer", "assumed")


def test_yaml_round_trip_and_the_graph_validates(tmp_path):
    csv = tmp_path / "data.csv"
    csv.write_text("id,shop,alta,hora,importe,activo\n1,001,12/07/2025,2025-07-12T10:20:30Z,10.5,true\n"
                   "2,002,31/01/2024,2024-01-31T08:00:00Z,3,false\n")
    df = read_dataframe(str(csv), dtype=str)
    result = infer_schema(df, "VENTAS")
    yaml_file = tmp_path / "ventas.yaml"
    yaml_file.write_text(schema_to_yaml(result.schema))

    loaded = load_schema(str(yaml_file))
    assert [(f.name, f.tipo, f.formato) for f in loaded.fields] == [
        ("id", "integer", None), ("shop", "string", None), ("alta", "date", "%d/%m/%Y"),
        ("hora", "datetime", "%Y-%m-%dT%H:%M:%SZ"), ("importe", "float", None), ("activo", "boolean", None)]

    graph = OntologyBuilder(build_global_tbox(), reasoner=None).build_abox_graph(loaded, df)
    data, shape = tmp_path / "d.ttl", tmp_path / "s.ttl"
    graph.serialize(destination=str(data), format="turtle")
    shape.write_text(generar_shape_shacl(loaded))
    report = validate_shacl_report(str(data), str(shape))
    assert report.conforms, report.text


def test_cli_writes_the_schema_and_lists_assumptions(tmp_path, capsys):
    csv = tmp_path / "in.csv"
    csv.write_text("cp,fecha\n41001,01/02/2025\n28013,03/04/2025\n")
    out = tmp_path / "out.yaml"
    schema_cli.main(["--csv", str(csv), "--name", "T", "--output", str(out)])
    printed = capsys.readouterr().out
    assert "cp: integer [assumed]" in printed and "fecha: date (%d/%m/%Y) [assumed]" in printed
    assert "Please confirm the assumed types: cp, fecha" in printed
    assert load_schema(str(out)).fields[1].formato == "%d/%m/%Y"
