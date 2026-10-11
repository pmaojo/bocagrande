"""Select the reasoner backend by name."""
from __future__ import annotations

from typing import Optional

from .reasoner import Reasoner

REASONERS = ("ontologos", "hermit", "none")
DEFAULT_REASONER = "ontologos"


def create_reasoner(name: str = DEFAULT_REASONER, *, hermit_jar: Optional[str] = None) -> Optional[Reasoner]:
    """Return the reasoner called ``name``; ``"none"`` returns ``None``.

    ``ontologos`` needs no Java. ``hermit`` runs the HermiT jar and needs a Java runtime.
    """
    key = name.lower()
    if key == "ontologos":
        from .ontologos_reasoner import OntoLogosReasoner

        return OntoLogosReasoner()
    if key == "hermit":
        from .hermit_runner import HERMIT_JAR, HermiTReasoner

        return HermiTReasoner(jar_path=hermit_jar or HERMIT_JAR)
    if key == "none":
        return None
    raise ValueError(f"Unknown reasoner '{name}'. Choose one of: {', '.join(REASONERS)}")


__all__ = ["create_reasoner", "REASONERS", "DEFAULT_REASONER"]
