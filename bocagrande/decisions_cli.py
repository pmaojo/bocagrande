"""Command line for the decision log: review accuracy per confidence level and mark outcomes."""
from __future__ import annotations

import argparse
import sys

from adapter.decision_log import DecisionLog, calibration_report, suggest_threshold


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect the Bocagrande decision log")
    sub = parser.add_subparsers(dest="command", required=True)

    report = sub.add_parser("report", help="accuracy per confidence band and a suggested threshold")
    report.add_argument("--log", required=True)
    report.add_argument("--purpose", help="only decisions made for this purpose")
    report.add_argument("--target-accuracy", type=float, default=0.95)
    report.add_argument("--min-samples", type=int, default=20)

    mark = sub.add_parser("mark", help="record whether a decision was correct")
    mark.add_argument("--log", required=True)
    mark.add_argument("--id", required=True, help="decision id from the log")
    group = mark.add_mutually_exclusive_group(required=True)
    group.add_argument("--correct", action="store_true")
    group.add_argument("--incorrect", action="store_true")
    mark.add_argument("--note", default="")

    listing = sub.add_parser("list", help="decisions still without an outcome")
    listing.add_argument("--log", required=True)
    listing.add_argument("--purpose")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    log = DecisionLog(args.log)

    if args.command == "mark":
        log.record_outcome(args.id, correct=args.correct, note=args.note)
        print(f"Recorded {args.id} as {'correct' if args.correct else 'incorrect'}")
    elif args.command == "list":
        records = log.read()
        done = {r["id"] for r in records if r["type"] == "outcome"}
        for r in records:
            if r["type"] == "decision" and r["id"] not in done and (not args.purpose or r["purpose"] == args.purpose):
                print(f"{r['id']}  {r['purpose'] or '-':<10} {r['key']}: {r['answer']} ({r['confidence']:.0%})")
    else:
        reviewed = log.reviewed(args.purpose)
        print(f"{len(reviewed)} reviewed decision(s)")
        for row in calibration_report(reviewed):
            accuracy = "-" if row["accuracy"] is None else f"{row['accuracy']:.0%}"
            print(f"  confidence {row['from']:.2f}-{row['to']:.2f}: n={row['n']:<4} accuracy {accuracy}")
        threshold = suggest_threshold(reviewed, args.target_accuracy, args.min_samples)
        if threshold is None:
            print(f"No threshold reaches {args.target_accuracy:.0%} accuracy with at least {args.min_samples} "
                  "reviewed decisions: keep the current one and review more results.")
        else:
            print(f"Suggested minimum confidence: {threshold:.2f} "
                  f"(>= {args.target_accuracy:.0%} accurate over at least {args.min_samples} reviewed decisions)")


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
