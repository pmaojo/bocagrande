import pytest

from adapter.contract_loader import load_data_contract


def test_load_data_contract(tmp_path):
    contract_yaml = tmp_path / "contract.yaml"
    contract_yaml.write_text(
        """
contract:
  name: demo
  version: 2.0
  owner: a@b.com

tables:
  - file: foo.csv
    schema: schema_yaml/CLIENTES.yaml
    primary_key: [id]
    unique:
      - [id]
"""
    )
    contract = load_data_contract(str(contract_yaml))
    assert contract.name == "demo"
    assert contract.tables[0].schema.primary_key == ["id"]
    assert contract.tables[0].schema.unique == [["id"]]


def _contract(tmp_path, unique_line):
    path = tmp_path / "contract.yaml"
    path.write_text(f"""
contract:
  name: demo
tables:
  - file: t.csv
    schema: schema_yaml/CLIENTES.yaml
{unique_line}
""")
    return path


def test_unique_must_be_a_list_of_column_names(tmp_path):
    with pytest.raises(TypeError, match="unique"):
        load_data_contract(str(_contract(tmp_path, "    unique: id")))
    with pytest.raises(TypeError):
        load_data_contract(str(_contract(tmp_path, "    unique: [[id], 3]")))


def test_a_flat_unique_list_is_one_composite_constraint(tmp_path):
    contract = load_data_contract(str(_contract(tmp_path, "    unique: [customerId, shop_code]")))
    assert contract.tables[0].schema.unique == [["customerId", "shop_code"]]


def test_a_missing_schema_is_reported_not_skipped(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text("contract:\n  name: demo\ntables:\n  - file: x.csv\n    schema: schema_yaml/NOPE.yaml\n")
    with pytest.raises(FileNotFoundError):
        load_data_contract(str(path))
