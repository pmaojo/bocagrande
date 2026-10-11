"""Confidence gate: act on a decision only when the model is sure, otherwise escalate."""
from __future__ import annotations

from typing import Collection, Optional

from adapter.decision import Decision


class DecisionGate:
    def __init__(self, min_confidence: float = 0.8) -> None:
        self.min_confidence = min_confidence

    def accepts(self, decision: Decision, allowed: Optional[Collection[str]] = None) -> bool:
        """True when the confidence reaches the threshold and the answer is one of ``allowed``."""
        if allowed is not None and decision.answer not in allowed:
            return False
        return decision.confidence >= self.min_confidence


__all__ = ["DecisionGate"]
