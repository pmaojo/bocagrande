"""Decision service backed by OpenRouter's Decisions API.

Serves ``inception/mercury-decide`` and Jev, which share the same schema.
Endpoint: POST https://openrouter.ai/api/alpha/decisions
Question types: ``noul`` (yes/no probability), ``choice`` and ``score``.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

import requests

from .decision import Decision, DecisionQuestion, DecisionService

DEFAULT_URL = "https://openrouter.ai/api/alpha/decisions"
DEFAULT_MODEL = "inception/mercury-decide:free"
_API_TYPES = {"yes_no": "noul", "choice": "choice", "score": "score"}


class OpenRouterDecisions(DecisionService):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        url: Optional[str] = None,
        session: Optional[requests.Session] = None,
        timeout: float = 30.0,
    ) -> None:
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        self.model = model or os.environ.get("DECISION_MODEL") or DEFAULT_MODEL
        self.url = url or os.environ.get("OPENROUTER_DECISIONS_URL") or DEFAULT_URL
        self.session = session or requests.Session()
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> Optional["OpenRouterDecisions"]:
        """Return a service when ``OPENROUTER_API_KEY`` is set, otherwise ``None``."""
        return cls() if os.environ.get("OPENROUTER_API_KEY") else None

    def decide(self, state: Any, questions: Dict[str, DecisionQuestion]) -> Dict[str, Decision]:
        payload_questions = {
            name: {"type": _API_TYPES[q.kind], "instructions": q.instructions, "criteria": q.criteria}
            for name, q in questions.items()
        }
        response = self.session.post(
            self.url,
            json={"model": self.model, "state": state, "questions": payload_questions},
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        body = response.json()
        answers = body.get("answers", body)
        missing = [name for name in questions if name not in answers]
        if missing:
            raise ValueError(f"Decision response is missing answers for: {missing}")
        return {name: self._parse(questions[name], answers[name]) for name in questions}

    def _parse(self, question: DecisionQuestion, answer: Dict[str, Any]) -> Decision:
        if question.kind == "yes_no":
            p_yes = float(answer["noul"])
            return Decision("yes_no", p_yes >= 0.5, {"true": p_yes, "false": 1.0 - p_yes},
                            self._confidence(answer, max(p_yes, 1.0 - p_yes)), self.model)
        raw = answer.get("probabilities") or {}
        if isinstance(raw, dict):
            probabilities = {k: float(v) for k, v in raw.items()}
        else:
            probabilities = {label: float(p) for label, p in zip(question.options or [], raw)}
        if question.kind == "choice":
            choice = answer["choice"]
            return Decision("choice", choice, probabilities,
                            self._confidence(answer, probabilities.get(choice, 0.0)), self.model)
        return Decision("score", float(answer["score"]), probabilities,
                        self._confidence(answer, max(probabilities.values(), default=0.0)), self.model)

    @staticmethod
    def _confidence(answer: Dict[str, Any], derived: float) -> float:
        """Use the model's own confidence when it reports one, else the top probability."""
        reported = answer.get("confidence")
        return float(reported) if isinstance(reported, (int, float)) else derived


__all__ = ["OpenRouterDecisions", "DEFAULT_MODEL", "DEFAULT_URL"]
