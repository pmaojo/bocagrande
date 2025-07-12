from pathlib import Path
import pytest

from adapter.yaml_loader import load_schema

SCHEMA_DIR = Path("schema_yaml")


def iter_schema_files():
    for path in SCHEMA_DIR.glob("*.yaml"):
        name = path.name.lower()
        if "metadatos" in name or name == "data_contract.yaml":
            continue
        yield path


@pytest.mark.parametrize("yaml_file", list(iter_schema_files()), ids=lambda p: p.name)
def test_schema_files_parse(yaml_file: Path) -> None:
    """Ensure all table schemas can be loaded with :func:`load_schema`."""
    schema = load_schema(str(yaml_file))
    assert schema is not None, f"Failed to load {yaml_file.name}"
    assert schema.fields, f"No fields parsed for {yaml_file.name}"
    for field in schema.fields:
        assert field.name, f"Field with empty name in {yaml_file.name}"
