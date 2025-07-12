import asyncio
import pytest
from adapter.hermit_runner import HermiTReasoner

class DummyProc:
    def __init__(self, stdout=b"Ontology is consistent", stderr=b"", returncode: int = 0):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode

    async def communicate(self):
        return self.stdout, self.stderr

    def kill(self):
        self.killed = True

async def fake_create_subprocess_exec(*args, **kwargs):
    return DummyProc()


@pytest.mark.asyncio
async def test_hermit_reasoner_success(monkeypatch):
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    reasoner = HermiTReasoner(jar_path="fake.jar")
    ok, logs = await reasoner._reason_async("dummy.owl")
    assert ok is True
    assert "Ontology is consistent" in logs


@pytest.mark.asyncio
async def test_hermit_reasoner_returncode_failure(monkeypatch):
    async def make_proc(*args, **kwargs):
        return DummyProc(returncode=1)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", make_proc)
    reasoner = HermiTReasoner(jar_path="fake.jar")
    ok, logs = await reasoner._reason_async("dummy.owl")
    assert ok is False


@pytest.mark.asyncio
async def test_hermit_reasoner_inconsistent(monkeypatch):
    async def make_proc(*args, **kwargs):
        return DummyProc(stdout=b"Ontology is inconsistent")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", make_proc)
    reasoner = HermiTReasoner(jar_path="fake.jar")
    ok, logs = await reasoner._reason_async("dummy.owl")
    assert ok is False
    assert "inconsistent" in logs.lower()


@pytest.mark.asyncio
async def test_hermit_reasoner_timeout(monkeypatch):
    class SlowProc(DummyProc):
        async def communicate(self):
            await asyncio.sleep(0.2)
            return b"", b""

    captured = {}

    async def make_proc(*args, **kwargs):
        captured['proc'] = SlowProc()
        return captured['proc']

    monkeypatch.setattr(asyncio, "create_subprocess_exec", make_proc)
    reasoner = HermiTReasoner(jar_path="fake.jar")
    ok, logs = await reasoner._reason_async("dummy.owl", timeout=0.05)
    assert ok is False
    assert "timed out" in logs.lower()
    assert getattr(captured['proc'], 'killed', False) is True


def test_reason_with_loop(monkeypatch):
    async def make_proc(*args, **kwargs):
        return DummyProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", make_proc)
    reasoner = HermiTReasoner(jar_path="fake.jar")
    loop = asyncio.new_event_loop()
    try:
        ok, _ = reasoner.reason("dummy.owl", loop=loop)
    finally:
        loop.close()
    assert ok is True

