"""Map the columns of an input file to the fields of a target schema.

Deterministic code goes first (names that match after normalisation). Only the fields
left over are put to a decision model, in a single request, and only answers it is
confident about are used. Everything else is returned as ``pending`` so a person, or a
language model, can decide.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import pandas as pd

from adapter.decision import DecisionQuestion, DecisionService
from bocagrande.decision_gate import DecisionGate
from bocagrande.transform import ETLStep
from ontology.model import PropertyDef, TableSchema

logger = logging.getLogger(__name__)

NONE_OPTION = "NONE"
SOURCE_NAME, SOURCE_DECISION, SOURCE_FALLBACK = "name", "decision", "fallback"


@dataclass
class MappingResult:
    steps: List[ETLStep] = field(default_factory=list)
    sources: Dict[str, str] = field(default_factory=dict)  # campo_salida -> name | decision | fallback
    confidence: Dict[str, float] = field(default_factory=dict)  # only for decision-model mappings
    pending: List[str] = field(default_factory=list)  # fields nobody could map confidently


def normalize(name: str) -> str:
    """Lower-case, accent-free and alphanumeric-only, so ``Fecha_Nac`` equals ``fechaNac``."""
    text = unicodedata.normalize("NFKD", str(name))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _describe_field(schema_field: PropertyDef) -> dict:
    info = {"name": schema_field.name, "type": schema_field.tipo}
    description = schema_field.metadata.get("description") or schema_field.metadata.get("Descripción")
    if description:
        info["description"] = description
    if schema_field.formato:
        info["format"] = schema_field.formato
    return info


def _describe_headers(headers: List[str], sample: Optional[pd.DataFrame], sample_rows: int) -> Dict[str, object]:
    options: Dict[str, object] = {}
    for header in headers:
        entry: Dict[str, object] = {"what": f"Input column '{header}'"}
        if sample is not None and header in sample.columns:
            values = sample[header].dropna().astype(str).unique()[:sample_rows].tolist()
            if values:
                entry["example_values"] = values
        options[header] = entry
    options[NONE_OPTION] = {"what": "No input column holds this field"}
    return options


def suggest_steps(
    headers: List[str],
    schema: TableSchema,
    decisions: Optional[DecisionService] = None,
    *,
    sample: Optional[pd.DataFrame] = None,
    sample_rows: int = 3,
    send_samples: bool = False,
    min_confidence: float = 0.8,
    fallback: Optional[Callable[[List[str], TableSchema], List[ETLStep]]] = None,
) -> MappingResult:
    """Build mapping steps for ``schema``.

    ``send_samples`` controls whether example values from ``sample`` are sent to the
    decision service. It is off by default: only column names and field descriptions leave
    the machine unless you opt in.
    """
    gate = DecisionGate(min_confidence)
    result = MappingResult()
    used: set = set()
    chosen: Dict[str, ETLStep] = {}

    # 1. Deterministic: same name after normalisation
    by_name: Dict[str, str] = {}
    for header in headers:
        by_name.setdefault(normalize(header), header)
    for schema_field in schema.fields:
        header = by_name.get(normalize(schema_field.name))
        if header is not None and header not in used:
            used.add(header)
            chosen[schema_field.name] = ETLStep(campo_salida=schema_field.name, campo_entrada=header)
            result.sources[schema_field.name] = SOURCE_NAME

    # 2. Decision model for what is left, all fields in one request
    remaining_fields = [f for f in schema.fields if f.name not in chosen]
    remaining_headers = [h for h in headers if h not in used]
    if decisions is not None and remaining_fields and remaining_headers:
        _decide(remaining_fields, remaining_headers, decisions, gate, chosen, used, result,
                sample if send_samples else None, sample_rows)

    # 3. Escalate the rest to the fallback (e.g. a language model), if there is one
    pending = [f.name for f in schema.fields if f.name not in chosen]
    if pending and fallback is not None:
        try:
            for step in fallback(headers, schema):
                if step.campo_salida in pending:
                    chosen[step.campo_salida] = step
                    result.sources[step.campo_salida] = SOURCE_FALLBACK
        except Exception as exc:
            logger.error("Fallback mapping failed: %s", exc)

    result.steps = [chosen[f.name] for f in schema.fields if f.name in chosen]
    result.pending = [f.name for f in schema.fields if f.name not in chosen]
    return result


def _decide(fields, headers, decisions, gate, chosen, used, result, sample, sample_rows) -> None:
    header_options = _describe_headers(headers, sample, sample_rows)
    questions = {
        f.name: DecisionQuestion.choice(
            {
                "question": "Which input column holds the values for the output field?",
                "output_field": _describe_field(f),
            },
            header_options,
        )
        for f in fields
    }
    try:
        answers = decisions.decide({"input_columns": headers}, questions)
    except Exception as exc:
        logger.error("Decision model unavailable, leaving %d fields pending: %s", len(fields), exc)
        return

    accepted = [
        (name, d)
        for name, d in answers.items()
        if d.answer != NONE_OPTION and gate.accepts(d, allowed=headers)
    ]
    # A column can feed only one field: the most confident answer wins.
    for name, decision in sorted(accepted, key=lambda item: item[1].confidence, reverse=True):
        if decision.answer in used:
            continue
        used.add(decision.answer)
        chosen[name] = ETLStep(campo_salida=name, campo_entrada=decision.answer)
        result.sources[name] = SOURCE_DECISION
        result.confidence[name] = decision.confidence


__all__ = ["MappingResult", "suggest_steps", "normalize"]
