"""Infrastructure: wrapper for running the HermiT reasoner."""
from typing import Tuple
import asyncio
import os

from .reasoner import Reasoner

HERMIT_JAR = os.path.join("HermiT", "HermiT.jar")

class HermiTReasoner:
    """Reasoner implementation that delegates to the HermiT JAR."""

    def __init__(self, jar_path: str = HERMIT_JAR) -> None:
        self.jar_path = jar_path

    async def _reason_async(self, owl_path: str) -> Tuple[bool, str]:
        cmd = ["java", "-jar", self.jar_path, owl_path]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        logs = stdout.decode() + stderr.decode()
        ok = "Ontology is consistent" in logs or "consistent" in logs.lower()
        return ok, logs

    def reason(self, owl_path: str) -> Tuple[bool, str]:
        """Synchronously run the reasoner on ``owl_path``."""
        return asyncio.run(self._reason_async(owl_path))

__all__ = ["HermiTReasoner"]
