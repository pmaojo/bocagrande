"""Command line: infer a YAML schema from a CSV or Excel file."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from adapter.data_io import read_dataframe
from adapter.decision_log import create_decisions
from bocagrande.schema_inference import infer_schema, schema_to_yaml


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Infer a Bocagrande YAML schema from a data file")
    parser.add_argument("--csv", required=True, help="CSV or Excel file")
    parser.add_argument("--name", required=True, help="Table name")
    parser.add_argument("--output", required=True, help="YAML file to write")
    parser.add_argument("--month-first", action="store_true",
                        help="Read ambiguous dates as month/day/year (default: day/month/year)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    df = read_dataframe(args.csv, dtype=str)  # keep the values exactly as written
    result = infer_schema(df, args.name, create_decisions("schema"), prefer_day_first=not args.month_first)
    Path(args.output).write_text(schema_to_yaml(result.schema))

    for info in result.columns.values():
        detail = f" ({info.formato})" if info.formato else ""
        note = f" - {info.note}" if info.note else ""
        print(f"{info.name}: {info.tipo}{detail} [{info.source}]{note}")
    if result.assumed:
        print("\nPlease confirm the assumed types: " + ", ".join(result.assumed))
    print(f"Wrote {args.output}")


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
