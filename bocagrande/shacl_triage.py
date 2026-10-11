"""Classify SHACL violations by their most likely cause, so people fix the right thing.

Violations are grouped by (field, constraint): thousands of rows failing the same way are one
decision, not thousands. Code decides the clear cases from the numbers; a decision model is
asked only about the rest, in one request, and answers below the confidence threshold are
left for a person.

Categories
----------
data_error     some rows hold bad values: fix or remove those rows
schema_error   the schema constraint is wrong or too strict for correct data
mapping_error  the wrong column feeds the field, or no column does
needs_review   not enough evidence to say
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from adapter.decision import DecisionQuestion, DecisionService
from adapter.shacl_runner import Violation
from bocagrande.decision_gate import DecisionGate
from ontology.model import PropertyDef, TableSchema
from ontology.utils import limpiar_para_uri

logger = logging.getLogger(__name__)

DATA_ERROR = "data_error"
SCHEMA_ERROR = "schema_error"
MAPPING_ERROR = "mapping_error"
NEEDS_REVIEW = "needs_review"
CATEGORIES = (DATA_ERROR, SCHEMA_ERROR, MAPPING_ERROR)

SOURCE_RULE, SOURCE_DECISION, SOURCE_ESCALATED, SOURCE_NO_MODEL = "rule", "decision", "escalated", "no_model"

_CATEGORY_CRITERIA = {
    DATA_ERROR: {
        "what": "Some rows contain wrong, malformed or missing values; the constraint itself is reasonable",
        "signals": ["Only some rows fail", "Failing values look like typos, junk or placeholders"],
        "not_for": "Nearly every row failing in the same way",
    },
    SCHEMA_ERROR: {
        "what": "The constraint is too strict or wrong for values that look correct",
        "signals": [
            "Most or all rows fail the same way",
            "Failing values look like valid data in another format or size",
        ],
        "not_for": "A handful of rows with obviously wrong values",
    },
    MAPPING_ERROR: {
        "what": "The wrong input column feeds this field, or no column does",
        "signals": [
            "Values look like they belong to a different field",
            "The field is empty in every row",
        ],
        "not_for": "Values that fit the field but break one rule",
    },
}


@dataclass
class ViolationGroup:
    path: str  # property local name in the graph
    component: str  # Datatype, MinCount, MaxLength, Pattern...
    field: Optional[PropertyDef]
    violations: int
    rows: int  # distinct rows affected
    total_rows: int
    examples: List[str] = field(default_factory=list)

    @property
    def share(self) -> float:
        return self.rows / self.total_rows if self.total_rows else 0.0

    @property
    def field_name(self) -> str:
        return self.field.name if self.field else self.path


@dataclass
class Triage:
    group: ViolationGroup
    category: str
    source: str
    confidence: Optional[float]  # None when the code decided
    reason: str

    @property
    def action(self) -> str:
        name, comp = self.group.field_name, self.group.component
        if self.category == DATA_ERROR:
            return f"Fix or remove the {self.group.rows} affected row(s) for '{name}'."
        if self.category == SCHEMA_ERROR:
            return f"Review the {comp} constraint of '{name}' in the schema."
        if self.category == MAPPING_ERROR:
            return f"Check which input column feeds '{name}'."
        return f"Inspect '{name}' ({comp}) manually."


def group_violations(
    violations: Sequence[Violation],
    schema: TableSchema,
    total_rows: int,
    *,
    max_examples: int = 5,
) -> List[ViolationGroup]:
    by_local = {limpiar_para_uri(f.name.upper()): f for f in schema.fields}
    focus: Dict[Tuple[str, str], set] = defaultdict(set)
    counts: Dict[Tuple[str, str], int] = defaultdict(int)
    examples: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for v in violations:
        key = (v.path, v.component)
        counts[key] += 1
        focus[key].add(v.focus_node)
        if v.value is not None and v.value not in examples[key] and len(examples[key]) < max_examples:
            examples[key].append(v.value)
    groups = [
        ViolationGroup(path, comp, by_local.get(path), counts[(path, comp)], len(focus[(path, comp)]),
                       total_rows, examples[(path, comp)])
        for (path, comp) in counts
    ]
    return sorted(groups, key=lambda g: g.rows, reverse=True)


def _rule(group: ViolationGroup, isolated_share: float) -> Optional[Tuple[str, str]]:
    if group.share <= isolated_share:
        return DATA_ERROR, f"only {group.rows} of {group.total_rows} rows fail"
    if group.component == "MinCount" and group.rows == group.total_rows:
        return MAPPING_ERROR, "required field has no value in any row: no column is probably mapped to it"
    return None


def _question(group: ViolationGroup, send_samples: bool) -> DecisionQuestion:
    violation = {
        "constraint": group.component,
        "rows_affected": group.rows,
        "total_rows": group.total_rows,
        "share_of_rows": round(group.share, 3),
    }
    if send_samples and group.examples:
        violation["example_values"] = group.examples
    f = group.field
    field_info = {"name": group.field_name}
    if f is not None:
        field_info.update({"type": f.tipo, "required": f.requerido})
        for key, value in (("length", f.length), ("format", f.formato)):
            if value:
                field_info[key] = value
        description = f.metadata.get("description") or f.metadata.get("Descripción")
        if description:
            field_info["description"] = description
    return DecisionQuestion.choice(
        {"question": "What is the most likely cause of this group of SHACL violations?",
         "violation": violation, "field": field_info},
        _CATEGORY_CRITERIA,
    )


def triage(
    groups: Sequence[ViolationGroup],
    decisions: Optional[DecisionService] = None,
    *,
    isolated_share: float = 0.02,
    min_confidence: float = 0.8,
    send_samples: bool = False,
) -> List[Triage]:
    """Classify each group. ``send_samples`` lets example values reach the decision service."""
    gate = DecisionGate(min_confidence)
    results: Dict[int, Triage] = {}
    undecided: List[int] = []

    for i, group in enumerate(groups):
        ruled = _rule(group, isolated_share)
        if ruled:
            results[i] = Triage(group, ruled[0], SOURCE_RULE, None, ruled[1])
        else:
            undecided.append(i)

    answers = {}
    if decisions is not None and undecided:
        try:
            answers = decisions.decide(
                {"table_violations": len(groups)},
                {str(i): _question(groups[i], send_samples) for i in undecided},
            )
        except Exception as exc:
            logger.error("Decision model unavailable, %d group(s) need review: %s", len(undecided), exc)

    for i in undecided:
        group, decision = groups[i], answers.get(str(i))
        if decision is not None and gate.accepts(decision, allowed=CATEGORIES):
            results[i] = Triage(group, decision.answer, SOURCE_DECISION, decision.confidence,
                                f"decision model, {decision.confidence:.0%} confident")
        elif decision is not None:
            results[i] = Triage(group, NEEDS_REVIEW, SOURCE_ESCALATED, decision.confidence,
                                f"model not confident enough ({decision.confidence:.0%})")
        else:
            reason = "no decision model configured" if decisions is None else "decision model unavailable"
            results[i] = Triage(group, NEEDS_REVIEW, SOURCE_NO_MODEL, None, reason)
    return [results[i] for i in range(len(groups))]


__all__ = [
    "ViolationGroup", "Triage", "group_violations", "triage",
    "DATA_ERROR", "SCHEMA_ERROR", "MAPPING_ERROR", "NEEDS_REVIEW",
]
