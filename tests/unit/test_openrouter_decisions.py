from unittest.mock import MagicMock

import pytest

from adapter.decision import DecisionQuestion
from adapter.openrouter_decisions import DEFAULT_MODEL, DEFAULT_URL, OpenRouterDecisions


def service(answers):
    session = MagicMock()
    session.post.return_value.json.return_value = {"answers": answers}
    return OpenRouterDecisions(api_key="sk-or-test", session=session), session


def test_choice_request_and_parsing():
    svc, session = service({"q": {"type": "choice", "choice": "csv", "probabilities": {"csv": 0.7, "xls": 0.3}}})
    result = svc.decide("file", {"q": DecisionQuestion.choice({"question": "?"}, {"csv": {"what": "a"}, "xls": {"what": "b"}})})
    assert result["q"].answer == "csv" and result["q"].confidence == pytest.approx(0.7)
    call = session.post.call_args
    assert call.args[0] == DEFAULT_URL
    assert call.kwargs["headers"]["Authorization"] == "Bearer sk-or-test"
    body = call.kwargs["json"]
    assert body["model"] == DEFAULT_MODEL
    assert body["questions"]["q"]["type"] == "choice"
    assert body["questions"]["q"]["criteria"]["csv"] == {"what": "a"}


def test_yes_no_confidence_is_symmetric():
    svc, _ = service({"q": {"type": "noul", "noul": 0.1}})
    d = svc.decide("s", {"q": DecisionQuestion.yes_no("q")})["q"]
    assert d.answer is False and d.confidence == pytest.approx(0.9)


def test_reported_confidence_wins():
    svc, _ = service({"q": {"type": "choice", "choice": "a", "probabilities": {"a": 0.9}, "confidence": 0.55}})
    assert svc.decide("s", {"q": DecisionQuestion.choice("q", ["a"])})["q"].confidence == pytest.approx(0.55)


def test_score_accepts_list_probabilities():
    svc, _ = service({"q": {"type": "score", "score": 1.4, "probabilities": [0.1, 0.5, 0.4]}})
    d = svc.decide("s", {"q": DecisionQuestion.score("q", ["Calm", "Annoyed", "Angry"])})["q"]
    assert d.answer == pytest.approx(1.4) and d.probabilities["Annoyed"] == pytest.approx(0.5)


def test_several_questions_use_one_request():
    svc, session = service({
        "a": {"type": "noul", "noul": 0.9},
        "b": {"type": "choice", "choice": "x", "probabilities": {"x": 1.0}},
    })
    svc.decide("s", {"a": DecisionQuestion.yes_no("q"), "b": DecisionQuestion.choice("q", ["x"])})
    assert session.post.call_count == 1


def test_missing_answer_and_http_errors_raise():
    svc, _ = service({})
    with pytest.raises(ValueError):
        svc.decide("s", {"a": DecisionQuestion.yes_no("q")})
    session = MagicMock()
    session.post.return_value.raise_for_status.side_effect = RuntimeError("500")
    with pytest.raises(RuntimeError):
        OpenRouterDecisions(api_key="k", session=session).decide("s", {"a": DecisionQuestion.yes_no("q")})


def test_from_env_requires_a_key(monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    assert OpenRouterDecisions.from_env() is None
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    monkeypatch.setenv("DECISION_MODEL", "typesafe/jev-router")
    svc = OpenRouterDecisions.from_env()
    assert svc is not None and svc.model == "typesafe/jev-router"
