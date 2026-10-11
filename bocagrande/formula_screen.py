"""Static screen for formulas written by a language model, applied before ``DataFrame.eval``.

Only arithmetic, comparisons, boolean logic, constants, a short list of pure math functions
and a short list of ``.str`` / ``.dt`` accessors are accepted. This is a screen, not a sandbox.
"""
from __future__ import annotations

import ast
import re
from typing import Tuple

ALLOWED_FUNCTIONS = {
    "abs", "sqrt", "log", "log10", "exp", "floor", "ceil", "sin", "cos", "tan",
    "arcsin", "arccos", "arctan", "sinh", "cosh", "tanh", "expm1", "log1p",
}
ALLOWED_STR_METHODS = {
    "upper", "lower", "title", "strip", "lstrip", "rstrip", "len", "contains",
    "startswith", "endswith", "replace", "slice", "zfill", "capitalize",
}
ALLOWED_DT_MEMBERS = {
    "year", "month", "day", "hour", "minute", "second", "weekday", "dayofweek",
    "dayofyear", "quarter", "date",
}
_ALLOWED_NODES = (
    ast.Expression, ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare, ast.Constant,
    ast.Name, ast.Load, ast.Tuple, ast.List, ast.Call, ast.Attribute, ast.keyword,
    ast.operator, ast.unaryop, ast.boolop, ast.cmpop,
)
_BACKTICK = re.compile(r"`([^`]+)`")


def check_formula(formula: str) -> Tuple[bool, str]:
    """Return ``(True, "")`` when the formula only uses allowed constructs, else ``(False, reason)``."""
    if not formula or not formula.strip():
        return False, "empty formula"
    # pandas lets column names with spaces be written between backticks
    text = _BACKTICK.sub(lambda m: "__col__", formula)
    try:
        tree = ast.parse(text.strip(), mode="eval")
    except SyntaxError as exc:
        return False, f"syntax error: {exc.msg}"

    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            return False, f"'{type(node).__name__}' is not allowed"
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.MatMult):
            return False, "'@' is not allowed"
        if isinstance(node, ast.Name) and node.id.startswith("__") and node.id != "__col__":
            return False, f"name '{node.id}' is not allowed"
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                if func.id not in ALLOWED_FUNCTIONS:
                    return False, f"function '{func.id}' is not allowed"
            elif isinstance(func, ast.Attribute) and _accessor_chain(func.value):
                if func.attr not in ALLOWED_STR_METHODS:
                    return False, f"method '{func.attr}' is not allowed"
            else:
                return False, "only math functions and .str methods can be called"
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                if node.attr not in {"str", "dt"}:
                    return False, f"attribute '{node.attr}' is not allowed"
            elif not (_accessor_chain(node.value) and node.attr in ALLOWED_STR_METHODS | ALLOWED_DT_MEMBERS):
                return False, f"attribute '{node.attr}' is not allowed"
    return True, ""


def _accessor_chain(node: ast.AST) -> bool:
    """True for ``<column>.str`` or ``<column>.dt``."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr in {"str", "dt"}
        and isinstance(node.value, ast.Name)
    )


__all__ = ["check_formula"]
