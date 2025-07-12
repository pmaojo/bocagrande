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


def test_contract_table_order(tmp_path):
    contract_yaml = tmp_path / "contract.yaml"
    contract_yaml.write_text(
        """
contract:
  name: demo

tables:
  - file: first.csv
    schema: schema_yaml/CLIENTES.yaml
  - file: second.csv
    schema: schema_yaml/USUARIOS.yaml
"""
    )

    contract = load_data_contract(contract_yaml)
    assert [t.file for t in contract.tables] == ["first.csv", "second.csv"]
    assert len(contract.tables) == 2


def test_contract_invalid_unique_type(tmp_path):
    contract_yaml = tmp_path / "contract.yaml"
    contract_yaml.write_text(
        """
contract:
  name: demo

tables:
  - file: bad.csv
    schema: schema_yaml/CLIENTES.yaml
    unique: id
"""
    )

    with pytest.raises(TypeError):
        load_data_contract(contract_yaml)
