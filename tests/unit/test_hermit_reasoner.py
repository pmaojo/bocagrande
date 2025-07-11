import asyncio
import pytest
from adapter.hermit_runner import HermiTReasoner

class DummyProc:
    def __init__(self, stdout=b"Ontology is consistent", stderr=b""):
        self.stdout = stdout
        self.stderr = stderr
    async def communicate(self):
        return self.stdout, self.stderr

async def fake_create_subprocess_exec(*args, **kwargs):
    return DummyProc()


@pytest.mark.asyncio
async def test_hermit_reasoner_success(monkeypatch):
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    reasoner = HermiTReasoner(jar_path="fake.jar")
    ok, logs = await reasoner._reason_async("dummy.owl")
    assert ok is True
    assert "Ontology is consistent" in logs

