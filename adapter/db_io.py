"""Infrastructure: read from and write to databases (SQL through SQLAlchemy, and MongoDB).

Config keys
-----------
type                 postgresql | mysql | sqlite | mssql | oracle | mongodb (inferred from the URL if omitted)
connection_string    SQLAlchemy URL or MongoDB URI. ``${NAME}`` is replaced by the environment variable NAME,
                     so passwords never need to be written in a config file.
SQL read             ``query`` (with optional ``params``) or ``table_name`` (with optional ``schema``)
SQL write            ``table_name``, ``if_exists`` (fail | replace | append, default fail), ``index`` (default
                     False), ``chunksize``, ``schema``
MongoDB              ``database_name``, ``collection_name``; read also takes ``query_filter``; write also takes
                     ``if_exists``

Drivers are optional: install the one for your database (see requirements-db.txt). Error messages
never include the connection string, because it can hold a password.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from pandas.errors import DatabaseError as PandasDatabaseError
from sqlalchemy.exc import ArgumentError, NoSuchModuleError, SQLAlchemyError

SQL_TYPES = {"postgresql", "mysql", "sqlite", "mssql", "oracle"}
DB_TYPES = SQL_TYPES | {"mongodb"}
IF_EXISTS = ("fail", "replace", "append")
DRIVER_HINTS = {
    "postgresql": "psycopg[binary] (use postgresql+psycopg://)", "mysql": "pymysql (use mysql+pymysql://)",
    "mssql": "pyodbc (use mssql+pyodbc://)", "oracle": "oracledb (use oracle+oracledb://)",
    "mongodb": "pymongo",
}
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SCHEME = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*):")


class DatabaseError(Exception):
    """Base class for database errors raised here."""


class UnsupportedDatabaseError(DatabaseError):
    """The database type is unknown, or its driver is not installed."""


class DatabaseConnectionError(DatabaseError):
    """Connecting, querying or writing failed."""


class TargetExistsError(DatabaseError):
    """The target table or collection already holds data and ``if_exists`` is ``fail``."""


def expand_env(value: str) -> str:
    """Replace ``${NAME}`` with the environment variable NAME; an unset variable is an error."""
    def replace(match: re.Match) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(f"Environment variable '{name}' used in the connection string is not set")
        return os.environ[name]

    return _ENV_REF.sub(replace, value)


def redact(connection_string: str) -> str:
    """The connection string with any password hidden; never raises."""
    try:
        return make_url(connection_string).render_as_string(hide_password=True)
    except Exception:
        return "<invalid connection string>"


def _db_type(config: Dict[str, Any], connection_string: str) -> str:
    declared = config.get("type")
    if declared:
        db_type = str(declared).lower()
    else:
        found = _SCHEME.match(connection_string)
        if not found:
            raise ValueError("Invalid connection string: expected a URL such as sqlite:///file.db")
        db_type = found.group(1).lower().split("+", 1)[0]
    if db_type not in DB_TYPES:
        raise UnsupportedDatabaseError(f"Database type '{db_type}' is not supported. Use one of: {', '.join(sorted(DB_TYPES))}")
    return db_type


def _connection_string(config: Dict[str, Any]) -> str:
    raw = config.get("connection_string")
    if not raw:
        raise ValueError("Missing 'connection_string'")
    return expand_env(str(raw))


def _require(config: Dict[str, Any], *keys: str) -> None:
    for key in keys:
        if not config.get(key):
            raise ValueError(f"Missing '{key}'")


def _engine(connection_string: str, db_type: str):
    try:
        make_url(connection_string)
        return create_engine(connection_string)
    except ArgumentError:
        raise ValueError(f"Invalid connection string for {db_type}") from None
    except (ImportError, NoSuchModuleError) as exc:
        raise UnsupportedDatabaseError(
            f"The driver for {db_type} is not installed: pip install {DRIVER_HINTS.get(db_type, db_type)}"
        ) from exc


def _wrap(exc: Exception, connection_string: str, action: str) -> DatabaseError:
    """A database error whose message cannot leak the connection string."""
    detail = str(getattr(exc, "orig", None) or exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
    return DatabaseConnectionError(f"{action} failed on {redact(connection_string)}: {detail}")


def _mongo_client(connection_string: str):
    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise UnsupportedDatabaseError(f"The driver for mongodb is not installed: pip install {DRIVER_HINTS['mongodb']}") from exc
    return MongoClient(connection_string)


def read_database(config: Dict[str, Any]) -> pd.DataFrame:
    """Read a table, a query or a MongoDB collection into a DataFrame."""
    connection_string = _connection_string(config)
    db_type = _db_type(config, connection_string)

    if db_type == "mongodb":
        _require(config, "database_name", "collection_name")
        client = _mongo_client(connection_string)
        try:
            documents = list(client[config["database_name"]][config["collection_name"]].find(config.get("query_filter") or {}))
        except Exception as exc:
            raise _wrap(exc, connection_string, "Reading") from exc
        finally:
            client.close()
        df = pd.DataFrame(documents)
        if "_id" in df.columns:
            df["_id"] = df["_id"].astype(str)  # ObjectId is not usable downstream
        return df

    if not config.get("query") and not config.get("table_name"):
        raise ValueError("Provide 'query' or 'table_name'")
    engine = _engine(connection_string, db_type)
    try:
        with engine.connect() as connection:
            if config.get("query"):
                return pd.read_sql_query(text(config["query"]), connection, params=config.get("params"))
            return pd.read_sql_table(config["table_name"], connection, schema=config.get("schema"))
    except (SQLAlchemyError, PandasDatabaseError, ValueError, ImportError) as exc:
        if isinstance(exc, ImportError):
            raise UnsupportedDatabaseError(f"The driver for {db_type} is not installed: pip install {DRIVER_HINTS.get(db_type, db_type)}") from exc
        raise _wrap(exc, connection_string, "Reading") from exc
    finally:
        engine.dispose()


def write_database(df: pd.DataFrame, config: Dict[str, Any]) -> None:
    """Write ``df`` to a table or collection. SQL writes happen in one transaction."""
    connection_string = _connection_string(config)
    db_type = _db_type(config, connection_string)
    if_exists = config.get("if_exists", "fail")
    if if_exists not in IF_EXISTS:
        raise ValueError(f"Invalid 'if_exists': {if_exists}. Use one of: {', '.join(IF_EXISTS)}")

    if db_type == "mongodb":
        _require(config, "database_name", "collection_name")
        _write_mongo(df, config, connection_string, if_exists)
        return

    _require(config, "table_name")
    table, schema = config["table_name"], config.get("schema")
    engine = _engine(connection_string, db_type)
    try:
        with engine.begin() as connection:
            if if_exists == "fail" and inspect(connection).has_table(table, schema=schema):
                raise TargetExistsError(f"Table '{table}' already exists and if_exists is 'fail'")
            df.to_sql(table, con=connection, if_exists=if_exists, index=config.get("index", False),
                      chunksize=config.get("chunksize"), schema=schema)
    except DatabaseError:
        raise
    except (SQLAlchemyError, PandasDatabaseError, ValueError) as exc:
        raise _wrap(exc, connection_string, "Writing") from exc
    except ImportError as exc:
        raise UnsupportedDatabaseError(f"The driver for {db_type} is not installed: pip install {DRIVER_HINTS.get(db_type, db_type)}") from exc
    finally:
        engine.dispose()


def _write_mongo(df: pd.DataFrame, config: Dict[str, Any], connection_string: str, if_exists: str) -> None:
    client = _mongo_client(connection_string)
    try:
        collection = client[config["database_name"]][config["collection_name"]]
        records = df.to_dict(orient="records")
        if if_exists == "fail" and collection.count_documents({}) > 0:
            raise TargetExistsError(f"Collection '{config['collection_name']}' is not empty and if_exists is 'fail'")
        if if_exists == "replace":
            collection.drop()
        if records:
            collection.insert_many(records, ordered=False)
    except DatabaseError:
        raise
    except Exception as exc:
        raise _wrap(exc, connection_string, "Writing") from exc
    finally:
        client.close()


_REFLECTED_TYPES = (
    ("BOOL", "boolean"), ("TIMESTAMP", "datetime"), ("DATETIME", "datetime"), ("DATE", "date"),
    ("INT", "integer"), ("SERIAL", "integer"), ("NUMERIC", "float"), ("DECIMAL", "float"),
    ("FLOAT", "float"), ("DOUBLE", "float"), ("REAL", "float"), ("NUMBER", "float"),
)


def reflect_columns(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Columns of a SQL table as ``[{"name", "tipo", "length", "required"}]`` from the database's own types."""
    connection_string = _connection_string(config)
    db_type = _db_type(config, connection_string)
    if db_type == "mongodb":
        raise UnsupportedDatabaseError("MongoDB collections have no fixed column types to reflect")
    _require(config, "table_name")
    engine = _engine(connection_string, db_type)
    try:
        columns = inspect(engine).get_columns(config["table_name"], schema=config.get("schema"))
    except (SQLAlchemyError, PandasDatabaseError, ValueError) as exc:
        raise _wrap(exc, connection_string, "Reflecting") from exc
    finally:
        engine.dispose()

    result = []
    for column in columns:
        sql_type = str(column["type"]).upper()
        tipo = next((name for key, name in _REFLECTED_TYPES if key in sql_type), "string")
        length = getattr(column["type"], "length", None) if tipo == "string" else None
        result.append({"name": column["name"], "tipo": tipo, "length": length, "required": not column.get("nullable", True)})
    return result


__all__ = [
    "read_database", "write_database", "reflect_columns", "redact", "expand_env",
    "DatabaseError", "UnsupportedDatabaseError", "DatabaseConnectionError", "TargetExistsError", "DB_TYPES",
]
