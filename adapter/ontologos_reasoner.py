"""Infrastructure: reasoner backed by OntoLogos (a Rust reasoner, no Java required)."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Optional, Tuple


class OntoLogosReasoner:
    """Consistency checks through the ``ontologos`` package.

    OntoLogos reasons over OWL 2 profiles (RL/EL). When an ontology uses axioms outside
    what it can fully handle it reports ``complete=False``: no contradiction was found,
    but consistency is not guaranteed. With ``strict=True`` that case is treated as a failure.
    """

    def __init__(self, strict: bool = False) -> None:
        self.strict = strict

    def _check(self, owl_path: str) -> Tuple[bool, str]:
        import ontologos

        try:
            result = ontologos.Reasoner(path=owl_path, trusted=True).check_consistency()
        except Exception as exc:  # parse errors, resource limits, unsupported syntax
            return False, f"Reasoner error: {exc}"

        consistent = bool(result.get("consistent"))
        complete = bool(result.get("complete", True))
        logs = "Ontology is consistent" if consistent else "Ontology is inconsistent"
        if consistent and not complete:
            logs += (
                "\nWarning: reasoning was incomplete for some axioms; "
                "no contradiction was found but consistency is not guaranteed"
            )
        return consistent and (complete or not self.strict), logs

    def reason(
        self,
        owl_path: str,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        timeout: float | None = None,
    ) -> Tuple[bool, str]:
        """Synchronously check ``owl_path``. ``loop`` is accepted for interface parity."""
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(self._check, owl_path)
        try:
            return future.result(timeout=timeout)
        except FutureTimeout:
            return False, f"Reasoner timed out after {timeout} seconds"
        finally:
            executor.shutdown(wait=False)


__all__ = ["OntoLogosReasoner"]
