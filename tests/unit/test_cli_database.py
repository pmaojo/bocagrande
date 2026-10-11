import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from adapter.db_io import write_database
from adapter.yaml_loader import load_schema
from bocagrande import cli, schema_cli
from bocagrande.schema_inference import schema_from_columns, schema_to_yaml

SCHEMA = "table: CLIENTES\ncolumns:\n  - name: id\n    type: integer\n    required: true\n  - name: nombre\n    type: string\n"


@pytest.fixture
def database(tmp_path):
    url = f"sqlite:///{tmp_path / 'src.db'}"
    write_database(pd.DataFrame({"id": [1, 2, 3], "nombre": ["Ana", "Luis", "Eva"]}), {"connection_string": url, "table_name": "clientes"})
    schema = tmp_path / "clientes.yaml"
    schema.write_text(SCHEMA)
    return url, schema, tmp_path / "out.ttl"


def run(schema, out, *extra):
    cli.main(["--schema", str(schema), "--output", str(out), "--reasoner", "none", *extra])


def test_cli_reads_a_table(database):
    url, schema, out = database
    run(schema, out, "--db-url", url, "--table", "clientes")
    text_ = out.read_text()
    assert '"Ana"' in text_ and '"Eva"' in text_


def test_cli_reads_a_query(database):
    url, schema, out = database
    run(schema, out, "--db-url", url, "--query", "select * from clientes where id = 2")
    text_ = out.read_text()
    assert '"Luis"' in text_ and '"Ana"' not in text_


def test_cli_resolves_environment_references(database, monkeypatch):
    url, schema, out = database
    monkeypatch.setenv("BG_SRC", url.removeprefix("sqlite:///"))
    run(schema, out, "--db-url", "sqlite:///${BG_SRC}", "--table", "clientes")
    assert '"Ana"' in out.read_text()


@pytest.mark.parametrize("extra", [
    [],                                                                        # no source
    ["--csv", "x.csv", "--db-url", "sqlite://", "--table", "t"],                # two sources
    ["--db-url", "sqlite://"],                                                  # no query or table
    ["--db-url", "sqlite://", "--query", "select 1", "--table", "t"],           # both
    ["--csv", "x.csv", "--table", "t"],                                         # table without a database
])
def test_cli_rejects_inconsistent_sources(database, extra):
    _, schema, out = database
    with pytest.raises(SystemExit) as exit_:
        run(schema, out, *extra)
    assert exit_.value.code == 2


def test_cli_reports_database_errors_without_a_traceback_or_password(database):
    _, schema, out = database
    with pytest.raises(SystemExit) as exit_:
        run(schema, out, "--db-url", "sqlite:////nonexistent_dir/x.db", "--table", "t")
    assert "Could not read the database" in str(exit_.value)


def test_schema_from_a_table_uses_the_database_types(tmp_path, capsys):
    url = f"sqlite:///{tmp_path / 'r.db'}"
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("create table t (id integer not null, nombre varchar(30), alta date)"))
    engine.dispose()
    out = tmp_path / "t.yaml"
    schema_cli.main(["--db-url", url, "--table", "t", "--name", "T", "--output", str(out)])
    assert "id: integer required [database]" in capsys.readouterr().out
    schema = load_schema(str(out))
    assert [(f.name, f.tipo, f.requerido, f.length) for f in schema.fields] == [
        ("id", "integer", True, None), ("nombre", "string", False, 30), ("alta", "date", False, None)]


def test_schema_from_a_query_is_inferred_from_the_rows(database, tmp_path):
    url, _, _ = database
    out = tmp_path / "q.yaml"
    schema_cli.main(["--db-url", url, "--query", "select id, nombre from clientes", "--name", "Q", "--output", str(out)])
    assert [(f.name, f.tipo) for f in load_schema(str(out)).fields] == [("id", "integer"), ("nombre", "string")]


def test_schema_from_columns_round_trips_length_and_required(tmp_path):
    schema = schema_from_columns("T", [{"name": "a", "tipo": "string", "length": 5, "required": True}])
    path = tmp_path / "t.yaml"
    path.write_text(schema_to_yaml(schema))
    loaded = load_schema(str(path))
    assert (loaded.fields[0].length, loaded.fields[0].requerido) == (5, True)


def test_postgres_end_to_end(tmp_path):
    url = os.environ.get("BOCAGRANDE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set BOCAGRANDE_TEST_POSTGRES_URL to run against a real PostgreSQL")
    pytest.importorskip("psycopg")
    write_database(pd.DataFrame({"id": [1, 2], "nombre": ["Ana", "Luis"]}),
                   {"connection_string": url, "table_name": "bg_cli_clientes", "if_exists": "replace"})
    schema = tmp_path / "clientes.yaml"
    schema.write_text(SCHEMA)
    out = tmp_path / "pg.ttl"
    run(schema, out, "--db-url", url, "--table", "bg_cli_clientes")
    assert '"Luis"' in out.read_text()
    schema_cli.main(["--db-url", url, "--table", "bg_cli_clientes", "--name", "X", "--output", str(tmp_path / "x.yaml")])
    assert [f.tipo for f in load_schema(str(tmp_path / "x.yaml")).fields] == ["integer", "string"]
