import pytest

from adapter.ontologos_reasoner import OntoLogosReasoner

PREFIXES = """@prefix : <http://ex.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
:A a owl:Class . :B a owl:Class . :A owl:disjointWith :B .
"""


def write(tmp_path, name, body):
    path = tmp_path / name
    path.write_text(PREFIXES + body)
    return str(path)


def test_consistent_ontology_is_accepted(tmp_path):
    ok, logs = OntoLogosReasoner().reason(write(tmp_path, "good.ttl", ":x a :A . :y a :B ."))
    assert ok is True and "is consistent" in logs


def test_contradictory_ontology_is_rejected(tmp_path):
    ok, logs = OntoLogosReasoner().reason(write(tmp_path, "bad.ttl", ":x a :A , :B ."))
    assert ok is False and "inconsistent" in logs


def test_missing_file_reports_an_error_instead_of_raising(tmp_path):
    ok, logs = OntoLogosReasoner().reason(str(tmp_path / "missing.ttl"))
    assert ok is False and "Reasoner error" in logs


class FakeReasoner:
    def __init__(self, result, delay=0.0):
        self.result, self.delay = result, delay

    def check_consistency(self):
        import time

        time.sleep(self.delay)
        return self.result


def patch_ontologos(monkeypatch, result, delay=0.0):
    import ontologos

    monkeypatch.setattr(ontologos, "Reasoner", lambda **kwargs: FakeReasoner(result, delay))


def test_incomplete_reasoning_warns_but_passes_by_default(monkeypatch):
    patch_ontologos(monkeypatch, {"consistent": True, "complete": False})
    ok, logs = OntoLogosReasoner().reason("any.ttl")
    assert ok is True and "incomplete" in logs


def test_incomplete_reasoning_fails_in_strict_mode(monkeypatch):
    patch_ontologos(monkeypatch, {"consistent": True, "complete": False})
    ok, _ = OntoLogosReasoner(strict=True).reason("any.ttl")
    assert ok is False


def test_timeout_is_reported(monkeypatch):
    patch_ontologos(monkeypatch, {"consistent": True, "complete": True}, delay=0.5)
    ok, logs = OntoLogosReasoner().reason("any.ttl", timeout=0.05)
    assert ok is False and "timed out" in logs
