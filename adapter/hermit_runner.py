"""Infrastructure: wrapper for running the HermiT reasoner."""
from typing import Tuple, Optional
import asyncio
import os

from .reasoner import Reasoner

HERMIT_JAR = os.path.join("HermiT", "HermiT.jar")

class HermiTReasoner:
    """Reasoner implementation that delegates to the HermiT JAR."""

    def __init__(self, jar_path: str = HERMIT_JAR) -> None:
        self.jar_path = jar_path

    async def _reason_async(self, owl_path: str, *, timeout: float | None = None) -> Tuple[bool, str]:
        cmd = ["java", "-jar", self.jar_path, owl_path]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return False, f"Reasoner timed out after {timeout} seconds"
        logs = stdout.decode() + stderr.decode()
        lower_logs = logs.lower()
        ok = (
            proc.returncode == 0
            and "inconsistent" not in lower_logs
            and "consistent" in lower_logs
        )
        return ok, logs

    def reason(
        self,
        owl_path: str,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        timeout: float | None = None,
    ) -> Tuple[bool, str]:
        """Synchronously run the reasoner on ``owl_path``."""
        if loop is None:
            return asyncio.run(self._reason_async(owl_path, timeout=timeout))
        return loop.run_until_complete(self._reason_async(owl_path, timeout=timeout))

__all__ = ["HermiTReasoner"]
