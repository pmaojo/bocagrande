"""Command line: validate and run a pipeline file."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from bocagrande.pipeline import Context, PipelineError, load_pipeline, run_pipeline


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Bocagrande pipeline (YAML or JSON)")
    parser.add_argument("pipeline", help="pipeline file")
    parser.add_argument("--check", action="store_true", help="only validate the pipeline, do not run it")
    parser.add_argument("--param", action="append", default=[], metavar="NAME=VALUE", help="set a pipeline parameter")
    parser.add_argument("--base-dir", help="directory that relative paths resolve against (default: the pipeline's folder)")
    parser.add_argument("--sandbox", action="store_true", help="refuse file paths outside the base directory")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    try:
        pipeline = load_pipeline(args.pipeline)
    except (PipelineError, OSError) as exc:
        raise SystemExit(f"Could not load the pipeline: {exc}")

    problems = pipeline.validate()
    if problems:
        raise SystemExit("Invalid pipeline:\n  - " + "\n  - ".join(problems))
    if args.check:
        print(f"{pipeline.name or args.pipeline}: valid ({len(pipeline.nodes)} nodes, {len(pipeline.edges)} edges)")
        return

    parameters = dict(p.split("=", 1) for p in args.param if "=" in p)
    context = Context(base_dir=Path(args.base_dir or Path(args.pipeline).resolve().parent),
                      sandbox=args.sandbox, parameters=parameters)
    report = run_pipeline(pipeline, context)
    for node in report.nodes:
        print(f"[{node.status:7}] {node.id} ({node.type}) {node.message}")
    if report.status != "ok":
        raise SystemExit(1)


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
