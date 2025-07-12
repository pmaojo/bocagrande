import yaml
import pytest
from pathlib import Path

from adapter.yaml_loader import load_schema


def test_load_schema_success(tmp_path):
    yaml_content = """
table: CLIENTES
columns:
  - name: id
    type: integer
    length: 5
  - name: fecha
    type: date
    format: "%Y-%m-%d"
"""
    file = tmp_path / "clientes.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.name == "CLIENTES"
    assert schema.fields[0].name == "id"
    assert schema.fields[0].tipo == "integer"
    assert schema.fields[0].length == 5
    assert schema.fields[1].formato == "%Y-%m-%d"


def test_load_schema_unrecognized_returns_none(tmp_path):
    file = tmp_path / "unknown.yaml"
    file.write_text("foo: bar")
    assert load_schema(str(file)) is None


def test_load_schema_invalid_yaml(tmp_path):
    file = tmp_path / "bad.yaml"
    file.write_text("foo: [bar")
    with pytest.raises(yaml.YAMLError):
        load_schema(str(file))


def test_required_field_with_accent(tmp_path):
    yaml_content = """
table: TEST
columns:
  - name: nombre
    type: string
    required: Sí
"""
    file = tmp_path / "accent.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].requerido is True


def test_required_field_without_accent(tmp_path):
    yaml_content = """
table: TEST
columns:
  - name: nombre
    type: string
    required: Si
"""
    file = tmp_path / "noaccent.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].requerido is True


def test_length_4gb_is_none(tmp_path):
    yaml_content = """
table: TEST
columns:
  - name: obs
    type: string
    length: 4GB
"""
    file = tmp_path / "big.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].length is None


def test_precision_and_scale_parsed(tmp_path):
    yaml_content = """
table: TEST
columns:
  - name: amount
    type: float
    precision: 10
    scale: 2
"""
    file = tmp_path / "digits.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    field = schema.fields[0]
    assert field.precision == 10
    assert field.scale == 2


def test_precision_scale_from_length(tmp_path):
    yaml_content = """
table: TEST
columns:
  - name: amount
    type: float
    length: "20,6"
"""
    file = tmp_path / "length_digits.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    field = schema.fields[0]
    assert field.precision == 20
    assert field.scale == 6


def test_load_schema_unique(tmp_path):
    yaml_content = """
table: TEST
columns:
  - name: id
    type: integer
unique:
  - [id]
"""
    file = tmp_path / "unique.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.unique == [["id"]]


def test_gradgafa_formats_loaded():
    """Ensure that all Formato values from GRADGAFA.yaml are parsed correctly."""
    path = Path("schema_yaml/GRADGAFA.yaml")
    raw_schema = yaml.safe_load(path.read_text())
    raw_fields = raw_schema["columns"]
    schema = load_schema(str(path))
    assert schema is not None

    expected = {f["name"]: f.get("format") for f in raw_fields}
    loaded = {field.name: field.formato for field in schema.fields}
    assert expected == loaded


def test_clientes_date_formats_loaded():
    """Check Formato values for CLIENTES.yaml date fields."""
    path = Path("schema_yaml/CLIENTES.yaml")
    raw_schema = yaml.safe_load(path.read_text())
    raw_fields = raw_schema["columns"]
    schema = load_schema(str(path))
    assert schema is not None

    target = {"fechaNac", "ultVisita", "primeraVisita"}
    expected = {f["name"]: f.get("format") for f in raw_fields if f["name"] in target}
    loaded = {field.name: field.formato for field in schema.fields if field.name in target}
    assert expected == loaded
