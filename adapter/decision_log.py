"""Decision log: what a decision model answered, and whether it was right.

Every decision is appended to a JSONL file. The log records the question key (for example a field name),
the answer, the confidence and the model, but not the data that was sent. When someone reviews a
result, :meth:`DecisionLog.record_outcome` stores whether the decision was correct; with enough
outcomes :func:`calibration_report` shows how accurate each confidence level really is and
:func:`suggest_threshold` proposes a threshold from evidence instead of a guess.
"""
from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .decision import Decision, DecisionQuestion, DecisionService
from .openrouter_decisions import OpenRouterDecisions

DEFAULT_BINS: Tuple[float, ...] = (0.0, 0.5, 0.7, 0.8, 0.9, 1.0001)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DecisionLog:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = Lock()

    def _append(self, record: Dict[str, Any]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def record_decision(self, purpose: str, key: str, question: DecisionQuestion, decision: Decision) -> str:
        decision_id = uuid.uuid4().hex
        self._append({
            "type": "decision", "id": decision_id, "ts": _now(), "purpose": purpose, "key": key,
            "kind": decision.kind, "answer": decision.answer, "confidence": decision.confidence,
            "probabilities": decision.probabilities, "model": decision.model,
            "options": question.options,
        })
        return decision_id

    def record_outcome(self, decision_id: str, correct: bool, note: str = "") -> None:
        self._append({"type": "outcome", "id": decision_id, "ts": _now(), "correct": bool(correct), "note": note})

    def read(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        with self.path.open(encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def reviewed(self, purpose: Optional[str] = None) -> List[Tuple[float, bool]]:
        """``(confidence, was_correct)`` for every decision that has an outcome (the latest one wins)."""
        records = self.read()
        outcomes = {r["id"]: r["correct"] for r in records if r["type"] == "outcome"}
        return [
            (r["confidence"], outcomes[r["id"]])
            for r in records
            if r["type"] == "decision" and r["id"] in outcomes and (purpose is None or r["purpose"] == purpose)
        ]


class LoggingDecisionService(DecisionService):
    """Wraps a decision service and logs each decision it returns."""

    def __init__(self, inner: DecisionService, log: DecisionLog, purpose: str = "") -> None:
        self.inner, self.log, self.purpose = inner, log, purpose

    def decide(self, state: Any, questions: Dict[str, DecisionQuestion]) -> Dict[str, Decision]:
        answers = self.inner.decide(state, questions)
        for key, decision in answers.items():
            decision.decision_id = self.log.record_decision(self.purpose, key, questions[key], decision)
        return answers


def create_decisions(purpose: str = "") -> Optional[DecisionService]:
    """The decision service configured in the environment, or ``None``.

    ``OPENROUTER_API_KEY`` enables it; ``DECISION_LOG`` (a file path) adds the decision log.
    """
    service = OpenRouterDecisions.from_env()
    if service is None:
        return None
    path = os.environ.get("DECISION_LOG")
    return LoggingDecisionService(service, DecisionLog(path), purpose) if path else service


def calibration_report(
    reviewed: Sequence[Tuple[float, bool]],
    bins: Sequence[float] = DEFAULT_BINS,
) -> List[Dict[str, Any]]:
    """Accuracy per confidence band: ``[{"from", "to", "n", "accuracy"}]`` (accuracy is ``None`` when n == 0)."""
    rows = []
    for low, high in zip(bins[:-1], bins[1:]):
        hits = [ok for conf, ok in reviewed if low <= conf < high]
        rows.append({"from": low, "to": min(high, 1.0), "n": len(hits),
                     "accuracy": (sum(hits) / len(hits)) if hits else None})
    return rows


def suggest_threshold(
    reviewed: Sequence[Tuple[float, bool]],
    target_accuracy: float = 0.95,
    min_samples: int = 20,
) -> Optional[float]:
    """The lowest confidence at or above which decisions were at least ``target_accuracy`` correct.

    ``None`` when no threshold has ``min_samples`` reviewed decisions meeting the target.
    """
    for threshold in sorted({conf for conf, _ in reviewed}):
        kept = [ok for conf, ok in reviewed if conf >= threshold]
        if len(kept) >= min_samples and sum(kept) / len(kept) >= target_accuracy:
            return threshold
    return None


__all__ = [
    "DecisionLog", "LoggingDecisionService", "create_decisions", "calibration_report", "suggest_threshold",
]
