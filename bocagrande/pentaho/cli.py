"""``bocagrande-import-pentaho``: turn a Kettle ``.ktr`` (or an outline of a ``.kjb``) into a pipeline file."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from bocagrande.pentaho import import_file
from bocagrande.pentaho.ktr import PentahoParseError


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Import a Pentaho .ktr/.kjb as a Bocagrande pipeline")
    parser.add_argument("source", help=".ktr transformation or .kjb job")
    parser.add_argument("-o", "--output", help="pipeline file to write (YAML, or JSON if it ends in .json); default: stdout")
    parser.add_argument("--report", action="store_true", help="print the conversion report as JSON on stderr")
    args = parser.parse_args(argv)
    try:
        pipeline, report = import_file(Path(args.source))
    except PentahoParseError as exc:
        print(f"Cannot import {args.source}: {exc}", file=sys.stderr)
        return 2
    text = json.dumps(pipeline.to_dict(), indent=2) if (args.output or "").endswith(".json") else pipeline.to_yaml()
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text)
    if args.report:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False), file=sys.stderr)
    for item in report.unsupported:
        print(f"not converted: {item['step']} ({item['type']}): {item['reason']}", file=sys.stderr)
    for variable in report.environment:
        print(f"set before running: {variable}", file=sys.stderr)
    return 0 if report.complete else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
