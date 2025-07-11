from typing import Tuple, Protocol

class Reasoner(Protocol):
    """Interface for semantic reasoners."""

    def reason(self, owl_path: str) -> Tuple[bool, str]:
        """Run reasoning on the given OWL file."""
        ...

