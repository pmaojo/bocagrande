import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from adapter import db_io
from adapter.db_io import (
    DatabaseConnectionError, TargetExistsError, UnsupportedDatabaseError,
    expand_env, read_database, redact, reflect_columns, write_database,
)


def sqlite_url(tmp_path, name="t.db"):
    return f"sqlite:///{tmp_path / name}"


def frame():
    return pd.DataFrame({"id": [1, 2, 3], "nombre": ["Ana", "Luis", None], "saldo": [1.5, 2.5, 3.0]})


def test_round_trip_through_a_table_and_a_query(tmp_path):
    cfg = {"connection_string": sqlite_url(tmp_path), "table_name": "clientes"}
    write_database(frame(), cfg)
    assert read_database(cfg)["id"].tolist() == [1, 2, 3]
    big = read_database({"connection_string": cfg["connection_string"], "query": "select * from clientes where id >= :m",
                         "params": {"m": 2}})
    assert big["nombre"].tolist()[0] == "Luis" and len(big) == 2


def test_the_index_is_not_written_by_default(tmp_path):
    cfg = {"connection_string": sqlite_url(tmp_path), "table_name": "t"}
    write_database(frame(), cfg)
    assert list(read_database(cfg).columns) == ["id", "nombre", "saldo"]


def test_if_exists_modes(tmp_path):
    cfg = {"connection_string": sqlite_url(tmp_path), "table_name": "t"}
    write_database(frame(), cfg)
    with pytest.raises(TargetExistsError):
        write_database(frame(), cfg)                                  # default is fail
    write_database(frame(), {**cfg, "if_exists": "append"})
    assert len(read_database(cfg)) == 6
    write_database(frame().head(1), {**cfg, "if_exists": "replace"})
    assert len(read_database(cfg)) == 1


def test_a_failed_write_leaves_the_table_untouched(tmp_path):
    url = sqlite_url(tmp_path)
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("create table t (id integer primary key, nombre text)"))
        conn.execute(text("insert into t values (1, 'Ana')"))
    engine.dispose()
    clash = pd.DataFrame({"id": [2, 1], "nombre": ["Luis", "Duplicate"]})  # the second row violates the key
    with pytest.raises(DatabaseConnectionError):
        write_database(clash, {"connection_string": url, "table_name": "t", "if_exists": "append"})
    assert read_database({"connection_string": url, "table_name": "t"})["id"].tolist() == [1]


def test_values_are_bound_not_interpolated(tmp_path):
    cfg = {"connection_string": sqlite_url(tmp_path), "table_name": "t"}
    write_database(frame(), cfg)
    out = read_database({"connection_string": cfg["connection_string"], "query": "select * from t where nombre = :n",
                         "params": {"n": "x' OR '1'='1"}})
    assert out.empty


@pytest.mark.parametrize("cfg, message", [
    ({}, "connection_string"),
    ({"connection_string": "sqlite://"}, "query"),
])
def test_missing_configuration_is_a_value_error_not_a_connection_error(cfg, message):
    with pytest.raises(ValueError, match=message):
        read_database(cfg)


def test_write_configuration_errors(tmp_path):
    url = sqlite_url(tmp_path)
    with pytest.raises(ValueError, match="table_name"):
        write_database(frame(), {"connection_string": url})
    with pytest.raises(ValueError, match="if_exists"):
        write_database(frame(), {"connection_string": url, "table_name": "t", "if_exists": "explode"})


def test_an_invalid_url_does_not_echo_what_was_typed():
    with pytest.raises(ValueError) as error:
        read_database({"connection_string": "not a url with s3cret", "query": "select 1"})
    assert "s3cret" not in str(error.value)


def test_the_database_type_comes_from_the_url_and_unknown_types_are_rejected(tmp_path):
    assert db_io._db_type({}, "postgresql+psycopg://u@h/d") == "postgresql"
    assert db_io._db_type({}, "mongodb+srv://u@h") == "mongodb"
    with pytest.raises(UnsupportedDatabaseError, match="cassandra"):
        read_database({"connection_string": "cassandra://h/k", "query": "select 1"})


def test_a_missing_driver_says_what_to_install(monkeypatch):
    def no_driver(*args, **kwargs):
        raise ImportError("No module named 'oracledb'")

    monkeypatch.setattr(db_io, "create_engine", no_driver)
    with pytest.raises(UnsupportedDatabaseError, match="pip install oracledb"):
        read_database({"connection_string": "oracle+oracledb://u:p@h/db", "query": "select 1 from dual"})


def test_environment_references_keep_secrets_out_of_config_files(monkeypatch, tmp_path):
    monkeypatch.setenv("BG_DB_FILE", str(tmp_path / "env.db"))
    write_database(frame(), {"connection_string": "sqlite:///${BG_DB_FILE}", "table_name": "t"})
    assert (tmp_path / "env.db").exists()
    assert expand_env("a${BG_DB_FILE}") == f"a{tmp_path / 'env.db'}"
    monkeypatch.delenv("BG_UNSET_VARIABLE", raising=False)
    with pytest.raises(ValueError, match="BG_UNSET_VARIABLE"):
        expand_env("postgresql://u:${BG_UNSET_VARIABLE}@h/d")


def test_redact_hides_the_password_and_never_raises():
    assert "s3cret" not in redact("postgresql://user:s3cret@localhost/db")
    assert "user" in redact("postgresql://user:s3cret@localhost/db")
    assert redact("???") == "<invalid connection string>"


def test_connection_errors_never_contain_the_password():
    pytest.importorskip("psycopg")
    with pytest.raises(DatabaseConnectionError) as error:
        read_database({"connection_string": "postgresql+psycopg://user:s3cret@127.0.0.1:1/db",
                       "query": "select 1", "type": "postgresql"})
    assert "s3cret" not in str(error.value) and "s3cret" not in repr(error.value)


def test_reflected_columns_use_the_databases_own_types(tmp_path):
    url = sqlite_url(tmp_path)
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("create table t (id integer not null, nombre varchar(30), saldo float, activo boolean, "
                          "alta date, ts timestamp)"))
    engine.dispose()
    cols = {c["name"]: c for c in reflect_columns({"connection_string": url, "table_name": "t"})}
    assert [cols[n]["tipo"] for n in ("id", "nombre", "saldo", "activo", "alta", "ts")] == [
        "integer", "string", "float", "boolean", "date", "datetime"]
    assert cols["id"]["required"] is True and cols["nombre"]["required"] is False and cols["nombre"]["length"] == 30
    with pytest.raises(DatabaseConnectionError):
        reflect_columns({"connection_string": url, "table_name": "missing"})


# ---- MongoDB, against mongomock (no server needed)
@pytest.fixture
def mongo(monkeypatch):
    mongomock = pytest.importorskip("mongomock")
    pytest.importorskip("pymongo")
    import pymongo

    client = mongomock.MongoClient()
    monkeypatch.setattr(pymongo, "MongoClient", lambda *a, **k: client)
    return {"connection_string": "mongodb://localhost", "database_name": "d", "collection_name": "c"}


def test_mongodb_round_trip_and_object_ids_become_text(mongo):
    write_database(frame().dropna(), mongo)
    out = read_database(mongo)
    assert out["nombre"].tolist() == ["Ana", "Luis"] and out["_id"].map(type).eq(str).all()
    assert read_database({**mongo, "query_filter": {"id": 2}})["nombre"].tolist() == ["Luis"]


def test_mongodb_if_exists_modes(mongo):
    write_database(frame().dropna(), mongo)
    with pytest.raises(TargetExistsError):
        write_database(frame(), mongo)
    write_database(frame().dropna(), {**mongo, "if_exists": "append"})
    assert len(read_database(mongo)) == 4
    write_database(frame().head(1), {**mongo, "if_exists": "replace"})
    assert len(read_database(mongo)) == 1


def test_mongodb_needs_database_and_collection():
    with pytest.raises(ValueError, match="database_name"):
        read_database({"connection_string": "mongodb://localhost"})


# ---- a real PostgreSQL, when BOCAGRANDE_TEST_POSTGRES_URL points at one
@pytest.fixture
def postgres_url():
    url = os.environ.get("BOCAGRANDE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set BOCAGRANDE_TEST_POSTGRES_URL to run against a real PostgreSQL")
    pytest.importorskip("psycopg")
    return url


def test_postgres_round_trip_reflection_and_transactional_replace(postgres_url):
    cfg = {"connection_string": postgres_url, "table_name": "bg_test_clientes", "if_exists": "replace"}
    df = frame().assign(alta=pd.to_datetime(["2025-07-12"] * 3))
    write_database(df, cfg)
    out = read_database({"connection_string": postgres_url, "query": "select * from bg_test_clientes where id >= :m",
                         "params": {"m": 2}})
    assert out["id"].tolist() == [2, 3]
    cols = {c["name"]: c["tipo"] for c in reflect_columns(cfg)}
    assert cols == {"id": "integer", "nombre": "string", "saldo": "float", "alta": "datetime"}
    with pytest.raises(TargetExistsError):
        write_database(df, {**cfg, "if_exists": "fail"})
    write_database(df.head(1), cfg)                 # replace
    assert len(read_database({"connection_string": postgres_url, "table_name": "bg_test_clientes"})) == 1
