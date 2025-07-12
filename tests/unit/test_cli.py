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
    monkeypatch.setattr(cli, "HermiTReasoner", lambda jar_path: dummy)

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
