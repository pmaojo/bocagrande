import yaml
import pytest
from pathlib import Path

from adapter.yaml_loader import load_schema


def test_load_schema_success(tmp_path):
    yaml_content = """
fields:
  - Campo: id
    Tipo: integer
    Longitud: 5
  - Campo: fecha
    Tipo: date
    Formato: "%Y-%m-%d"
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
fields:
  - Campo: nombre
    Tipo: string
    Obligatorio: Sí
"""
    file = tmp_path / "accent.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].requerido is True


def test_required_field_without_accent(tmp_path):
    yaml_content = """
fields:
  - Campo: nombre
    Tipo: string
    Obligatorio: Si
"""
    file = tmp_path / "noaccent.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].requerido is True


def test_length_4gb_is_none(tmp_path):
    yaml_content = """
fields:
  - Campo: obs
    Tipo: string
    Longitud: 4GB
"""
    file = tmp_path / "big.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].length is None


def test_load_schema_unique(tmp_path):
    yaml_content = """
fields:
  - Campo: id
    Tipo: integer
unique:
  - [id]
"""
    file = tmp_path / "unique.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.unique == [["id"]]


def test_required_field_uppercase(tmp_path):
    yaml_content = """
fields:
  - Campo: nombre
    Tipo: string
    Obligatorio: SI
"""
    file = tmp_path / "upper.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].requerido is True


def test_decimal_length_ignored(tmp_path):
    yaml_content = """
fields:
  - Campo: price
    Tipo: decimal
    Longitud: 20,4
"""
    file = tmp_path / "decimal.yaml"
    file.write_text(yaml_content)
    schema = load_schema(str(file))
    assert schema is not None
    assert schema.fields[0].length is None


def test_gradgafa_formats_loaded():
    """Ensure that all Formato values from GRADGAFA.yaml are parsed correctly."""
    path = Path("schema_yaml/GRADGAFA.yaml")
    raw_fields = yaml.safe_load(path.read_text())
    schema = load_schema(str(path))
    assert schema is not None

    expected = {f["Campo"]: f.get("Formato") for f in raw_fields}
    loaded = {field.name: field.formato for field in schema.fields}
    assert expected == loaded


def test_clientes_date_formats_loaded():
    """Check Formato values for CLIENTES.yaml date fields."""
    path = Path("schema_yaml/CLIENTES.yaml")
    raw_fields = yaml.safe_load(path.read_text())
    schema = load_schema(str(path))
    assert schema is not None

    target = {"fechaNac", "ultVisita", "primeraVisita"}
    expected = {f["Campo"]: f.get("Formato") for f in raw_fields if f["Campo"] in target}
    loaded = {field.name: field.formato for field in schema.fields if field.name in target}
    assert expected == loaded
