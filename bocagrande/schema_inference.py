"""Infer a table schema (types and date formats) from a file that has none.

Code settles what the data itself shows: booleans, integers, floats, the date format a column
parses with, and identifiers with leading zeros. A decision model is asked only about one thing
the data cannot settle: whether a numeric column of fixed-length values (a postal code, a phone
number) is a quantity or an identifier. It sees the column name and statistics, never the values.

Read the file with ``dtype=str`` so pandas has not already altered the values (``001`` -> ``1``).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd
import yaml

from adapter.decision import DecisionQuestion, DecisionService
from bocagrande.decision_gate import DecisionGate
from ontology.model import PropertyDef, TableSchema

logger = logging.getLogger(__name__)

DATETIME_FORMATS = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M:%S"]
DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d.%m.%Y"]
_DAY_MONTH_PAIRS = {"%d/%m/%Y": "%m/%d/%Y", "%m/%d/%Y": "%d/%m/%Y",
                    "%d/%m/%Y %H:%M:%S": "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S": "%d/%m/%Y %H:%M:%S"}

_INTEGER = re.compile(r"^[+-]?\d+$")
_LEADING_ZERO = re.compile(r"^[+-]?0\d+$")
_FLOAT = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
MAX_SAFE_DIGITS = 15
MIN_IDENTIFIER_DIGITS = 5

SOURCE_RULE, SOURCE_DECISION, SOURCE_ASSUMED, SOURCE_DEFAULT = "rule", "decision", "assumed", "default"


@dataclass
class ColumnInference:
    name: str
    tipo: str
    formato: Optional[str] = None
    source: str = SOURCE_RULE  # rule | decision | assumed | default
    confidence: Optional[float] = None
    note: str = ""


@dataclass
class InferenceResult:
    schema: TableSchema
    columns: Dict[str, ColumnInference] = field(default_factory=dict)

    @property
    def assumed(self) -> List[str]:
        """Columns whose type is a guess a person should confirm."""
        return [c.name for c in self.columns.values() if c.source == SOURCE_ASSUMED]


def _parse_ratio(values: pd.Series, fmt: str) -> float:
    return float(pd.to_datetime(values, format=fmt, errors="coerce", exact=True).notna().mean())


def _date_candidates(values: pd.Series, formats: List[str], min_match: float) -> List[str]:
    return [fmt for fmt in formats if _parse_ratio(values, fmt) >= min_match]


def _infer_date(values: pd.Series, name: str, min_match: float, prefer_day_first: bool) -> Optional[ColumnInference]:
    for tipo, formats in (("datetime", DATETIME_FORMATS), ("date", DATE_FORMATS)):
        candidates = _date_candidates(values, formats, min_match)
        if not candidates:
            continue
        chosen = candidates[0]
        if len(candidates) > 1 and chosen in _DAY_MONTH_PAIRS and _DAY_MONTH_PAIRS[chosen] in candidates:
            day_first = [f for f in candidates if f.startswith("%d")]
            month_first = [f for f in candidates if f.startswith("%m")]
            chosen = (day_first if prefer_day_first else month_first)[0]
            return ColumnInference(name, tipo, chosen, SOURCE_ASSUMED, None,
                                   "day and month are both <= 12 in every value, so the order is a guess; "
                                   f"assumed {'day' if prefer_day_first else 'month'} first")
        return ColumnInference(name, tipo, chosen, SOURCE_RULE)
    return None


def _numeric_stats(values: pd.Series) -> Dict[str, object]:
    """Structure of a digit-only column. No value, minimum or maximum: with few rows those are the data."""
    lengths = values.str.len()
    numbers = pd.to_numeric(values).sort_values()
    consecutive = bool(len(numbers) > 1 and (numbers.diff().dropna() == 1).all())
    return {
        "values": int(len(values)),
        "distinct_ratio": round(float(values.nunique() / len(values)), 3),
        "digits_min": int(lengths.min()),
        "digits_max": int(lengths.max()),
        "consecutive_sequence": consecutive,
    }


def _infer_column(name: str, series: pd.Series, min_match: float, prefer_day_first: bool) -> ColumnInference:
    values = series.dropna().astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return ColumnInference(name, "string", source=SOURCE_DEFAULT, note="no values")

    if set(values) <= {"true", "false"}:
        return ColumnInference(name, "boolean")

    if values.map(lambda v: bool(_INTEGER.match(v))).all():
        if values.map(lambda v: bool(_LEADING_ZERO.match(v))).any():
            return ColumnInference(name, "string", note="leading zeros: an identifier, not a number")
        if values.str.lstrip("+-").str.len().max() > MAX_SAFE_DIGITS:
            return ColumnInference(name, "string", note=f"more than {MAX_SAFE_DIGITS} digits: an identifier")
        lengths = values.str.lstrip("+-").str.len()
        if lengths.nunique() == 1 and lengths.iloc[0] >= MIN_IDENTIFIER_DIGITS and len(values) > 1:
            return ColumnInference(name, "integer", source=SOURCE_ASSUMED,
                                   note="fixed-length digits: could be an identifier (postal code, phone) rather than a quantity")
        return ColumnInference(name, "integer")

    if values.map(lambda v: bool(_FLOAT.match(v))).all():
        return ColumnInference(name, "float")

    dated = _infer_date(values, name, min_match, prefer_day_first)
    if dated is not None:
        return dated
    return ColumnInference(name, "string", source=SOURCE_DEFAULT)


def infer_schema(
    df: pd.DataFrame,
    name: str,
    decisions: Optional[DecisionService] = None,
    *,
    min_match: float = 0.98,
    prefer_day_first: bool = True,
    min_confidence: float = 0.8,
) -> InferenceResult:
    """Infer a :class:`TableSchema` named ``name`` from the columns of ``df``."""
    columns = {c: _infer_column(c, df[c], min_match, prefer_day_first) for c in df.columns}

    ambiguous = [c for c, info in columns.items() if info.source == SOURCE_ASSUMED and info.tipo == "integer"]
    if decisions is not None and ambiguous:
        _settle_identifiers(df, name, ambiguous, columns, decisions, DecisionGate(min_confidence))

    fields = [PropertyDef(name=c, tipo=info.tipo, formato=info.formato) for c, info in columns.items()]
    return InferenceResult(TableSchema(name=name, fields=fields), columns)


def _settle_identifiers(df, table, names, columns, decisions, gate) -> None:
    options = {
        "quantity": {"what": "A number you could add or average: a count, an amount, a measurement",
                     "signals": ["Values vary in length", "Meaningful magnitude"],
                     "not_for": "Codes that happen to be made of digits"},
        "identifier": {"what": "A code made of digits that names something: postal code, phone, tax id, account number",
                       "signals": ["Fixed number of digits", "Arithmetic on it would be meaningless"],
                       "not_for": "Counts and amounts"},
    }
    questions = {
        c: DecisionQuestion.choice(
            {"question": "Is this digit-only column a quantity or an identifier?",
             "column": {"name": c, **_numeric_stats(df[c].dropna().astype(str).str.strip().replace("", pd.NA).dropna())}},
            options,
        )
        for c in names
    }
    try:
        answers = decisions.decide({"table": table}, questions)
    except Exception as exc:
        logger.error("Decision model unavailable, keeping %d assumed type(s): %s", len(names), exc)
        return
    for c, decision in answers.items():
        if not gate.accepts(decision, allowed=list(options)):
            continue
        info = columns[c]
        info.source, info.confidence = SOURCE_DECISION, decision.confidence
        info.tipo = "string" if decision.answer == "identifier" else "integer"
        info.note = f"decision model: {decision.answer}"


def schema_from_columns(name: str, columns: List[Dict[str, object]]) -> TableSchema:
    """A schema from columns reflected from a database table (``adapter.db_io.reflect_columns``).

    The database's own types are authoritative, so nothing is guessed: ``NOT NULL`` columns are required
    and ``VARCHAR(n)`` keeps its length.
    """
    return TableSchema(name=name, fields=[
        PropertyDef(name=str(c["name"]), tipo=str(c["tipo"]), requerido=bool(c.get("required")),
                    length=c.get("length") or None)
        for c in columns
    ])


def schema_to_yaml(schema: TableSchema) -> str:
    """Serialise ``schema`` in the canonical YAML layout that ``load_schema`` reads."""
    columns = []
    for f in schema.fields:
        column = {"name": f.name, "type": f.tipo}
        if f.formato:
            column["format"] = f.formato
        if f.length:
            column["length"] = f.length
        column["required"] = bool(f.requerido)
        columns.append(column)
    return yaml.safe_dump({"table": schema.name, "columns": columns}, allow_unicode=True, sort_keys=False)


__all__ = ["ColumnInference", "InferenceResult", "infer_schema", "schema_from_columns", "schema_to_yaml"]
