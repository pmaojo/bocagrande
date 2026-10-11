import json

import pytest

from adapter.decision import Decision, DecisionQuestion, DecisionService
from adapter.decision_log import (
    DecisionLog, LoggingDecisionService, calibration_report, create_decisions, suggest_threshold,
)
from bocagrande import decisions_cli


class Fixed(DecisionService):
    def decide(self, state, questions):
        return {k: Decision("choice", "a", {"a": 0.9}, 0.9, "m") for k in questions}


def question():
    return DecisionQuestion.choice({"question": "SECRET-INSTRUCTIONS"}, {"a": {"what": "x"}, "b": {"what": "y"}})


def test_each_decision_is_logged_and_gets_an_id(tmp_path):
    log = DecisionLog(tmp_path / "d.jsonl")
    service = LoggingDecisionService(Fixed(), log, purpose="mapping")
    result = service.decide({"secret": "SECRET-STATE"}, {"telefono": question()})["telefono"]
    (record,) = log.read()
    assert result.decision_id == record["id"]
    assert (record["type"], record["purpose"], record["key"], record["answer"]) == ("decision", "mapping", "telefono", "a")
    assert record["confidence"] == pytest.approx(0.9) and record["options"] == ["a", "b"]


def test_the_log_never_contains_the_data_that_was_sent(tmp_path):
    log = DecisionLog(tmp_path / "d.jsonl")
    LoggingDecisionService(Fixed(), log).decide({"secret": "SECRET-STATE"}, {"k": question()})
    assert "SECRET" not in (tmp_path / "d.jsonl").read_text()


def test_outcomes_are_joined_and_the_latest_wins(tmp_path):
    log = DecisionLog(tmp_path / "d.jsonl")
    ids = [d.decision_id for d in LoggingDecisionService(Fixed(), log, "p").decide({}, {"x": question(), "y": question()}).values()]
    log.record_outcome(ids[0], True)
    log.record_outcome(ids[1], True)
    log.record_outcome(ids[1], False, note="corrected")
    assert sorted(log.reviewed(), key=lambda t: t[1]) == [(0.9, False), (0.9, True)]
    assert log.reviewed("other") == []


def test_a_missing_log_reads_as_empty(tmp_path):
    assert DecisionLog(tmp_path / "nope.jsonl").read() == []


def test_calibration_report_bands():
    reviewed = [(0.55, False), (0.6, True), (0.85, True), (0.85, True), (0.95, True)]
    rows = {(r["from"], r["to"]): r for r in calibration_report(reviewed)}
    assert rows[(0.5, 0.7)]["n"] == 2 and rows[(0.5, 0.7)]["accuracy"] == pytest.approx(0.5)
    assert rows[(0.8, 0.9)]["accuracy"] == 1.0
    assert rows[(0.0, 0.5)]["n"] == 0 and rows[(0.0, 0.5)]["accuracy"] is None
    assert rows[(0.9, 1.0)]["n"] == 1


def test_suggest_threshold_needs_evidence():
    weak_then_strong = [(0.6, False)] * 10 + [(0.6, True)] * 10 + [(0.9, True)] * 30
    assert suggest_threshold(weak_then_strong, target_accuracy=0.95, min_samples=20) == 0.9
    assert suggest_threshold([(0.9, True)] * 5, min_samples=20) is None            # too few reviewed decisions
    assert suggest_threshold([(0.9, False)] * 30, target_accuracy=0.95) is None    # never accurate enough


def test_create_decisions_follows_the_environment(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    assert create_decisions("mapping") is None
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    monkeypatch.delenv("DECISION_LOG", raising=False)
    assert not isinstance(create_decisions("mapping"), LoggingDecisionService)
    monkeypatch.setenv("DECISION_LOG", str(tmp_path / "log.jsonl"))
    service = create_decisions("mapping")
    assert isinstance(service, LoggingDecisionService) and service.purpose == "mapping"


def test_cli_lists_marks_and_reports(tmp_path, capsys):
    path = tmp_path / "d.jsonl"
    log = DecisionLog(path)
    ids = [LoggingDecisionService(Fixed(), log, "mapping").decide({}, {"campo": question()})["campo"].decision_id
           for _ in range(25)]

    decisions_cli.main(["list", "--log", str(path)])
    assert ids[0] in capsys.readouterr().out

    for decision_id in ids:
        decisions_cli.main(["mark", "--log", str(path), "--id", decision_id, "--correct"])
    capsys.readouterr()
    decisions_cli.main(["list", "--log", str(path)])
    assert capsys.readouterr().out.strip() == ""

    decisions_cli.main(["report", "--log", str(path)])
    out = capsys.readouterr().out
    assert "25 reviewed" in out and "Suggested minimum confidence: 0.90" in out

    decisions_cli.main(["report", "--log", str(path), "--min-samples", "100"])
    assert "No threshold reaches" in capsys.readouterr().out
    assert json.loads(path.read_text().splitlines()[-1])["type"] == "outcome"
