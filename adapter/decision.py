"""Port for fast, typed "System One" decision models (Mercury Decide, Jev, ...).

These models do not generate text. They answer narrow, typed questions about a state and
return a calibrated probability, so code can act, retry or escalate on the confidence.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


@dataclass
class Decision:
    """A typed answer to one question."""

    kind: str  # "yes_no" | "choice" | "score"
    answer: Any  # bool for yes_no, option label for choice, float position for score
    probabilities: Dict[str, float] = field(default_factory=dict)
    confidence: float = 0.0
    model: Optional[str] = None
    decision_id: Optional[str] = None  # set by the decision log, used to record whether it was right


@dataclass
class DecisionQuestion:
    """One narrow question. ``instructions`` and ``criteria`` may be plain strings or structured."""

    kind: str  # "yes_no" | "choice" | "score"
    instructions: Any
    criteria: Any = None
    options: Optional[List[str]] = None  # choice labels, or score levels in order

    @staticmethod
    def yes_no(instructions: Any, true: Any = None, false: Any = None) -> "DecisionQuestion":
        return DecisionQuestion("yes_no", instructions, {
            "true": true or "The statement is true",
            "false": false or "The statement is not true",
        })

    @staticmethod
    def choice(instructions: Any, options: Union[List[str], Dict[str, Any]]) -> "DecisionQuestion":
        criteria = dict(options) if isinstance(options, dict) else {o: o for o in options}
        return DecisionQuestion("choice", instructions, criteria, list(criteria))

    @staticmethod
    def score(instructions: Any, levels: List[Any]) -> "DecisionQuestion":
        labels = [str(lv.get("what", i)) if isinstance(lv, dict) else str(lv) for i, lv in enumerate(levels)]
        return DecisionQuestion("score", instructions, list(levels), labels)


class DecisionService(ABC):
    @abstractmethod
    def decide(self, state: Any, questions: Dict[str, DecisionQuestion]) -> Dict[str, Decision]:
        """Answer independent questions about ``state`` in a single round trip."""


__all__ = ["Decision", "DecisionQuestion", "DecisionService"]
