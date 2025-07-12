"""Command-line interface for the Bocagrande ETL pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

from adapter.csv_loader import read_csv
from adapter.yaml_loader import load_schema
from adapter.hermit_runner import HermiTReasoner
from ontology.tbox_builder import build_global_tbox
from ontology.service import OntologyBuilder


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Bocagrande pipeline")
    parser.add_argument("--schema", required=True, help="YAML schema file")
    parser.add_argument("--csv", required=True, help="CSV data file")
    parser.add_argument("--output", required=True, help="Output TTL file")
    parser.add_argument(
        "--hermit",
        default=str(Path("HermiT") / "HermiT.jar"),
        help="Path to HermiT JAR",
    )
    parser.add_argument(
        "--skip-reasoner",
        action="store_true",
        help="Skip semantic reasoning",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    schema = load_schema(args.schema)
    if schema is None:
        raise SystemExit(f"Invalid schema: {args.schema}")

    df = pd.read_csv(args.csv)

    tbox = build_global_tbox(Path(args.schema).parent)

    reasoner = None if args.skip_reasoner else HermiTReasoner(jar_path=args.hermit)
    builder = OntologyBuilder(tbox, reasoner=reasoner)
    graph = builder.build_abox_graph(schema, df)

    if not args.skip_reasoner:
        ok, logs = builder.reason_graph(graph)
        print(logs)
        if not ok:
            raise SystemExit(1)

    graph.serialize(destination=args.output, format="turtle")
    print(f"Wrote {args.output}")


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    main(sys.argv[1:])
