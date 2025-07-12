from adapter import shacl_runner


def test_validate_shacl_success(monkeypatch):
    def fake_validate(**kwargs):
        return True, None, "OK"

    monkeypatch.setattr(shacl_runner, "validate", fake_validate)
    ok, logs = shacl_runner.validate_shacl("data.ttl", "shape.ttl")
    assert ok is True
    assert logs == "OK"


def test_validate_shacl_error(monkeypatch):
    def boom(**kwargs):
        raise Exception("fail")

    monkeypatch.setattr(shacl_runner, "validate", boom)
    ok, logs = shacl_runner.validate_shacl("data.ttl")
    assert ok is False
    assert "Error en validaci" in logs
