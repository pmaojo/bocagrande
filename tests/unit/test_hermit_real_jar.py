"""Runs the bundled HermiT jar for real; skipped when Java is not installed."""
import shutil
from pathlib import Path

import pytest

from adapter.hermit_runner import HermiTReasoner

pytestmark = pytest.mark.skipif(shutil.which("java") is None, reason="Java is not installed")

PREFIXES = """@prefix : <http://ex.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
:A a owl:Class . :B a owl:Class . :A owl:disjointWith :B .
"""


def run(tmp_path: Path, body: str):
    path = tmp_path / "o.ttl"
    path.write_text(PREFIXES + body)
    return HermiTReasoner().reason(str(path))


def test_consistent_ontology_passes(tmp_path):
    ok, logs = run(tmp_path, ":x a :A . :y a :B .")
    assert ok is True, logs


def test_contradictory_ontology_fails(tmp_path):
    ok, logs = run(tmp_path, ":x a :A , :B .")
    assert ok is False
    assert "inconsistent" in logs.lower()
