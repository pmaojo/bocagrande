"""Command-line interface for the Bocagrande ETL pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from adapter.data_io import read_dataframe
from adapter.db_io import DatabaseError, read_database
from bocagrande.dtypes import string_dtypes
from adapter.yaml_loader import load_schema
from adapter.reasoner_factory import DEFAULT_REASONER, REASONERS, create_reasoner
from ontology.tbox_builder import build_global_tbox
from ontology.service import OntologyBuilder


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Bocagrande pipeline")
    parser.add_argument("--schema", required=True, help="YAML schema file")
    source = parser.add_argument_group("data source (use one)")
    source.add_argument("--csv", help="CSV, Excel or Parquet data file")
    source.add_argument("--db-url", help="database URL, e.g. sqlite:///data.db or postgresql+psycopg://user:${DB_PASSWORD}@host/db "
                                         "(${NAME} is read from the environment)")
    source.add_argument("--query", help="SQL query to read (with --db-url)")
    source.add_argument("--table", help="table to read (with --db-url)")
    parser.add_argument("--output", required=True, help="Output TTL file")
    parser.add_argument(
        "--reasoner",
        choices=REASONERS,
        default=DEFAULT_REASONER,
        help="Reasoner backend: ontologos (default, no Java), hermit (needs Java) or none",
    )
    parser.add_argument(
        "--hermit",
        default=str(Path("HermiT") / "HermiT.jar"),
        help="Path to HermiT JAR (only with --reasoner hermit)",
    )
    parser.add_argument(
        "--skip-reasoner",
        action="store_true",
        help="Skip semantic reasoning (same as --reasoner none)",
    )
    args = parser.parse_args(argv)
    if bool(args.csv) == bool(args.db_url):
        parser.error("give exactly one data source: --csv or --db-url")
    if args.db_url and bool(args.query) == bool(args.table):
        parser.error("with --db-url give exactly one of --query or --table")
    if not args.db_url and (args.query or args.table):
        parser.error("--query and --table need --db-url")
    return args


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    schema = load_schema(args.schema)
    if schema is None:
        raise SystemExit(f"Invalid schema: {args.schema}")

    if args.db_url:
        config = {"connection_string": args.db_url, **({"query": args.query} if args.query else {"table_name": args.table})}
        try:
            df = read_database(config)
        except DatabaseError as exc:
            raise SystemExit(f"Could not read the database: {exc}")
    else:
        df = read_dataframe(args.csv, dtype=string_dtypes(schema))

    tbox = build_global_tbox(Path(args.schema).parent)

    reasoner = None if args.skip_reasoner else create_reasoner(args.reasoner, hermit_jar=args.hermit)
    builder = OntologyBuilder(tbox, reasoner=reasoner)
    graph = builder.build_abox_graph(schema, df)

    if reasoner is not None:
        ok, logs = builder.reason_graph(graph)
        print(logs)
        if not ok:
            raise SystemExit(1)

    graph.serialize(destination=args.output, format="turtle")
    print(f"Wrote {args.output}")


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    main(sys.argv[1:])
