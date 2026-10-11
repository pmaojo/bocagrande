"""The shapes must agree with the graph OntologyBuilder writes. These tests run real pyshacl."""
import pandas as pd
import pytest
import rdflib

from adapter.shacl_runner import validate_shacl_report
from adapter.yaml_to_shacl import generar_shape_shacl
from ontology.model import PropertyDef, TableSchema
from ontology.service import OntologyBuilder
from ontology.tbox_builder import build_global_tbox


def run(schema, df, tmp_path):
    graph = OntologyBuilder(build_global_tbox(), reasoner=None).build_abox_graph(schema, df)
    data, shape = tmp_path / "data.ttl", tmp_path / "shape.ttl"
    graph.serialize(destination=str(data), format="turtle")
    shape.write_text(generar_shape_shacl(schema))
    return validate_shacl_report(str(data), str(shape))


def people_schema():
    return TableSchema(name="clientes", fields=[
        PropertyDef(name="id", tipo="integer", requerido=True),
        PropertyDef(name="nombre", tipo="string", requerido=True, length=10),
        PropertyDef(name="saldo", tipo="float", precision=8, scale=2),
        PropertyDef(name="alta", tipo="date", formato="%Y-%m-%d"),
        PropertyDef(name="ultima", tipo="datetime"),
        PropertyDef(name="cp", tipo="string"),
    ])


def good_frame(n=5):
    return pd.DataFrame({
        "id": list(range(1, n + 1)),
        "nombre": ["Ana"] * n,
        "saldo": [10.5] * n,
        "alta": ["2025-07-12"] * n,
        "ultima": ["2025-07-12T10:20:30Z"] * n,
        "cp": [41001] * n,  # pandas reads postal codes as integers; the schema says string
    })


def test_good_data_conforms_with_lowercase_names_dates_and_precision(tmp_path):
    report = run(people_schema(), good_frame(), tmp_path)
    assert report.conforms, report.text
    assert report.violations == []


def test_shapes_with_patterns_are_valid_turtle():
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        schema = TableSchema(name="T", fields=[PropertyDef(name="d", tipo="date", formato=fmt),
                                               PropertyDef(name="x", tipo="float", precision=6, scale=2)])
        rdflib.Graph().parse(data=generar_shape_shacl(schema), format="turtle")


def test_missing_required_value_is_reported(tmp_path):
    df = good_frame()
    df.loc[2, "nombre"] = None
    report = run(people_schema(), df, tmp_path)
    assert [(v.path, v.component) for v in report.violations] == [("NOMBRE", "MinCount")]


def test_too_long_value_is_reported_with_the_value(tmp_path):
    df = good_frame()
    df.loc[1, "nombre"] = "A very long name"
    (violation,) = run(people_schema(), df, tmp_path).violations
    assert (violation.path, violation.component, violation.value) == ("NOMBRE", "MaxLength", "A very long name")


def test_wrong_datatype_is_reported(tmp_path):
    df = good_frame()
    df["id"] = df["id"].astype(object)
    df.loc[0, "id"] = "abc"
    (violation,) = run(people_schema(), df, tmp_path).violations
    assert (violation.path, violation.component, violation.value) == ("ID", "Datatype", "abc")


def test_date_in_the_wrong_format_is_reported(tmp_path):
    df = good_frame()
    df.loc[0, "alta"] = "12/07/2025"
    report = run(people_schema(), df, tmp_path)
    assert {v.path for v in report.violations} == {"ALTA"} and not report.conforms


def test_dates_are_parsed_with_the_schema_format(tmp_path):
    schema = TableSchema(name="T", fields=[
        PropertyDef(name="alta", tipo="date", formato="%d/%m/%Y"),
        PropertyDef(name="hora", tipo="datetime", formato="%d/%m/%Y %H:%M"),
    ])
    df = pd.DataFrame({"alta": ["12/07/2025", "01/02/2024"], "hora": ["12/07/2025 10:30", "01/02/2024 08:00"]})
    graph = OntologyBuilder(build_global_tbox(), reasoner=None).build_abox_graph(schema, df)
    lexical = {str(o) for _, p, o in graph if str(p).endswith(("ALTA", "HORA"))}
    assert lexical == {"2025-07-12T00:00:00", "2024-02-01T00:00:00", "2025-07-12T10:30:00", "2024-02-01T08:00:00"}
    assert run(schema, df, tmp_path).conforms


def test_a_date_that_does_not_match_the_format_is_left_alone_and_reported(tmp_path):
    schema = TableSchema(name="T", fields=[PropertyDef(name="alta", tipo="date", formato="%d/%m/%Y")])
    report = run(schema, pd.DataFrame({"alta": ["12/07/2025", "not a date"]}), tmp_path)
    assert [(v.path, v.component, v.value) for v in report.violations] == [("ALTA", "Datatype", "not a date")]


def test_datetime_objects_are_written_in_iso_form():
    from ontology.utils import a_xsd_datetime

    assert a_xsd_datetime(pd.Timestamp("2025-07-12 10:30:00")) == "2025-07-12T10:30:00"
    assert a_xsd_datetime("2025-07-12") == "2025-07-12T00:00:00"
    assert a_xsd_datetime("2025-07-12T10:20:30Z") == "2025-07-12T10:20:30Z"
