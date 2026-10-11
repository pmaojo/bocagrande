import io

import pytest

from adapter.data_io import read_dataframe
from bocagrande import cli
from bocagrande.dtypes import string_dtypes
from bocagrande.transform import ETLStep
from ontology.model import PropertyDef, TableSchema


def schema():
    return TableSchema(name="T", fields=[
        PropertyDef("tienda", "string"), PropertyDef("importe", "float"), PropertyDef("cp", "string"),
    ])


def test_string_fields_are_read_as_text_by_name():
    assert string_dtypes(schema()) == {"tienda": str, "cp": str}


def test_with_steps_the_input_columns_are_used():
    steps = [ETLStep("tienda", "shop_code"), ETLStep("importe", "amount"), ETLStep("cp", formula="1", tipo_transformacion="formula")]
    assert string_dtypes(schema(), steps) == {"shop_code": str}


def test_read_dataframe_keeps_leading_zeros_only_when_asked():
    data = b"tienda,importe\n001,1.5\n002,2.5\n"
    plain = read_dataframe(io.BytesIO(data))
    typed = read_dataframe(io.BytesIO(data), dtype={"tienda": str})
    assert list(plain["tienda"]) == [1, 2]
    assert list(typed["tienda"]) == ["001", "002"] and typed["importe"].dtype.kind == "f"


def test_unknown_columns_in_dtype_are_ignored():
    df = read_dataframe(io.BytesIO(b"a\n1\n"), dtype={"missing": str})
    assert list(df["a"]) == [1]


def test_cli_keeps_the_exact_text_in_the_graph(tmp_path):
    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text("table: T\ncolumns:\n  - name: tienda\n    type: string\n  - name: importe\n    type: float\n")
    csv_file = tmp_path / "t.csv"
    csv_file.write_text("tienda,importe\n001,1.5\n")
    out = tmp_path / "out.ttl"

    cli.main(["--schema", str(yaml_file), "--csv", str(csv_file), "--output", str(out), "--reasoner", "none"])

    text = out.read_text()
    assert '"001"' in text and '"1"' not in text
