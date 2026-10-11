import os
import pytest
from bocagrande import cli

class DummyReasoner:
    def __init__(self):
        self.called = False

    def reason(self, owl_path: str, *, loop=None, timeout=None):
        assert os.path.exists(owl_path)
        self.called = True
        return True, "ok"


def test_main_runs_pipeline(tmp_path, monkeypatch):
    yaml_file = tmp_path / "clientes.yaml"
    yaml_file.write_text("fields:\n  - Campo: nombre")
    csv_file = tmp_path / "clientes.csv"
    csv_file.write_text("nombre\nAlice\n")
    out_file = tmp_path / "out.ttl"

    dummy = DummyReasoner()
    monkeypatch.setattr(cli, "create_reasoner", lambda name, hermit_jar=None: dummy)

    cli.main([
        "--schema",
        str(yaml_file),
        "--csv",
        str(csv_file),
        "--output",
        str(out_file),
    ])

    assert out_file.exists()
    assert dummy.called


def test_main_skip_reasoner_validates_csv(tmp_path):
    yaml_file = tmp_path / "clientes.yaml"
    yaml_file.write_text("fields:\n  - Campo: nombre")
    csv_file = tmp_path / "clientes.csv"
    csv_file.write_text("nombre\n")
    out_file = tmp_path / "out.ttl"

    with pytest.raises(ValueError):
        cli.main([
            "--schema",
            str(yaml_file),
            "--csv",
            str(csv_file),
            "--output",
            str(out_file),
            "--skip-reasoner",
        ])


def test_main_default_reasoner_runs_end_to_end_without_java(tmp_path):
    out_file = tmp_path / "clientes.ttl"

    cli.main([
        "--schema", "schema_yaml/CLIENTES.yaml",
        "--csv", "raw/CLIENTES_sample.csv",
        "--output", str(out_file),
    ])

    assert out_file.exists() and out_file.stat().st_size > 0


def test_main_reasoner_none_skips_reasoning(tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr(cli, "create_reasoner", lambda name, hermit_jar=None: called.append(name))
    out_file = tmp_path / "out.ttl"

    cli.main([
        "--schema", "schema_yaml/CLIENTES.yaml",
        "--csv", "raw/CLIENTES_sample.csv",
        "--output", str(out_file),
        "--reasoner", "none",
    ])

    assert out_file.exists()
