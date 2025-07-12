import yaml
import pytest

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
