"""Command line: infer a YAML schema from a CSV or Excel file."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from adapter.data_io import read_dataframe
from adapter.db_io import DatabaseError, read_database, reflect_columns
from adapter.decision_log import create_decisions
from bocagrande.schema_inference import infer_schema, schema_from_columns, schema_to_yaml


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Infer a Bocagrande YAML schema from a data file")
    source = parser.add_argument_group("data source (use one)")
    source.add_argument("--csv", help="CSV or Excel file")
    source.add_argument("--db-url", help="database URL (${NAME} is read from the environment)")
    source.add_argument("--table", help="table to read the types from (with --db-url): uses the database's own types")
    source.add_argument("--query", help="SQL query to infer from (with --db-url): types are inferred from the rows")
    parser.add_argument("--name", required=True, help="Table name")
    parser.add_argument("--output", required=True, help="YAML file to write")
    parser.add_argument("--month-first", action="store_true",
                        help="Read ambiguous dates as month/day/year (default: day/month/year)")
    args = parser.parse_args(argv)
    if bool(args.csv) == bool(args.db_url):
        parser.error("give exactly one data source: --csv or --db-url")
    if args.db_url and bool(args.query) == bool(args.table):
        parser.error("with --db-url give exactly one of --query or --table")
    return args


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.db_url and args.table:
        try:
            columns = reflect_columns({"connection_string": args.db_url, "table_name": args.table})
        except DatabaseError as exc:
            raise SystemExit(f"Could not read the table: {exc}")
        Path(args.output).write_text(schema_to_yaml(schema_from_columns(args.name, columns)))
        for c in columns:
            print(f"{c['name']}: {c['tipo']}{' required' if c['required'] else ''} [database]")
        print(f"Wrote {args.output}")
        return

    if args.db_url:
        try:
            df = read_database({"connection_string": args.db_url, "query": args.query}).astype("string")
        except DatabaseError as exc:
            raise SystemExit(f"Could not read the database: {exc}")
    else:
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
