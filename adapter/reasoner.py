from typing import Tuple, Protocol, Optional
import asyncio

class Reasoner(Protocol):
    """Interface for semantic reasoners."""

    def reason(
        self,
        owl_path: str,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        timeout: float | None = None,
    ) -> Tuple[bool, str]:
        """Run reasoning on the given OWL file."""
        ...

