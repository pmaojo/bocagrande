import pandas as pd
import pytest

from bocagrande.formula_screen import check_formula
from bocagrande.transform import ETLStep, apply_transformations


@pytest.mark.parametrize("formula", [
    "a + b",
    "a * 2 - b / 3",
    "(a > 1) & (b < 5)",
    "a > 1 and b < 5",
    "sqrt(a) + abs(b)",
    "nombre.str.upper()",
    "nombre.str.replace('a', 'b')",
    "fecha.dt.year",
    "`first name` + `last name`",
    "a in [1, 2, 3]",
])
def test_safe_formulas_are_allowed(formula):
    assert check_formula(formula) == (True, "")


@pytest.mark.parametrize("formula", [
    "__import__('os').system('id')",
    "open('/etc/passwd')",
    "eval('1')",
    "a.__class__",
    "nombre.str.__len__()",
    "a.values",
    "a.apply(print)",
    "a @ b",
    "[x for x in a]",
    "lambda: 1",
    "os.getcwd()",
    "",
    "a +",
])
def test_unsafe_or_invalid_formulas_are_rejected(formula):
    allowed, reason = check_formula(formula)
    assert allowed is False and reason


def test_apply_transformations_still_evaluates_safe_formulas():
    df = pd.DataFrame({"a": [1, 2], "b": [10, 20], "nombre": ["ana", "luis"]})
    steps = [
        ETLStep("total", tipo_transformacion="formula", formula="a + b"),
        ETLStep("nombre_mayus", tipo_transformacion="calculo", formula="nombre.str.upper()"),
    ]
    out, generados, _, faltantes = apply_transformations(df, steps)
    assert list(out["total"]) == [11, 22]
    assert list(out["nombre_mayus"]) == ["ANA", "LUIS"]
    assert generados == ["total", "nombre_mayus"] and faltantes == []


def test_apply_transformations_never_runs_a_rejected_formula(tmp_path):
    marker = tmp_path / "pwned"
    df = pd.DataFrame({"a": [1]})
    step = ETLStep("x", tipo_transformacion="formula", formula=f"__import__('pathlib').Path(r'{marker}').touch()")
    out, generados, _, faltantes = apply_transformations(df, [step])
    assert not marker.exists()
    assert faltantes == ["x"] and generados == [] and out["x"].isnull().all()
