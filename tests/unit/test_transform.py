import pandas as pd
from bocagrande.transform import ETLStep, apply_transformations


def test_apply_transformations_mapping_and_formula():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    steps = [
        ETLStep(campo_salida="x", campo_entrada="a"),
        ETLStep(campo_salida="suma", tipo_transformacion="formula", formula="a + b"),
    ]
    result, generados, sobrescritos, faltantes = apply_transformations(df, steps)
    assert list(result.columns) == ["x", "suma"]
    assert generados == ["x", "suma"]
    assert sobrescritos == []
    assert faltantes == []
    assert result["x"].tolist() == [1, 2]
    assert result["suma"].tolist() == [4, 6]
