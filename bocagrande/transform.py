"""Utility functions for applying ETL transformations.

This module is intentionally separate from the Streamlit UI so
transformations can be unit tested in isolation.

The ``ETLStep`` dataclass represents a single transformation step.
``apply_transformations`` returns a new ``DataFrame`` with the columns
specified by the steps in the order they appear.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, List
import pandas as pd


@dataclass
class ETLStep:
    """Represent a single transformation step."""

    campo_salida: str
    campo_entrada: Optional[str] = None
    tipo_transformacion: str = "mapping"
    formula: Optional[str] = None
    descripcion: str | None = None


def apply_transformations(
    df: pd.DataFrame,
    steps: Iterable[ETLStep],
    *,
    overwrite: bool = True,
) -> tuple[pd.DataFrame, list[str], list[str], list[str]]:
    """Return ``(df_result, generados, sobrescritos, faltantes)``.

    Parameters
    ----------
    df:
        Input dataframe containing the raw columns.
    steps:
        Iterable of transformation steps describing how to build
        the output columns.
    overwrite:
        If ``True`` later steps can overwrite existing columns.

    Returns
    -------
    tuple
        ``DataFrame`` with the transformed columns and three lists with
        generated, overwritten and missing column names.
    """
    result = pd.DataFrame()
    generados: list[str] = []
    sobrescritos: list[str] = []
    faltantes: list[str] = []
    for step in steps:
        out_col = step.campo_salida
        if not out_col:
            continue

        if step.tipo_transformacion in {"mapping", "map"} and step.campo_entrada:
            if step.campo_entrada in df.columns:
                if overwrite or out_col not in result.columns or result[out_col].isnull().all():
                    result[out_col] = df[step.campo_entrada]
                    if out_col in df.columns:
                        sobrescritos.append(out_col)
                    else:
                        generados.append(out_col)
                else:
                    result[out_col] = result[out_col].combine_first(df[step.campo_entrada])
                    generados.append(out_col)
            else:
                result[out_col] = None
                faltantes.append(out_col)
        elif step.tipo_transformacion in {"calculo", "formula"} and step.formula:
            try:
                result[out_col] = df.eval(step.formula)
                generados.append(out_col)
            except Exception:
                result[out_col] = None
                faltantes.append(out_col)
        else:
            result[out_col] = None
            faltantes.append(out_col)

    columns: List[str] = [s.campo_salida for s in steps if s.campo_salida]
    for col in columns:
        if col not in result.columns:
            result[col] = None
    return result[columns], generados, sobrescritos, faltantes

__all__ = ["ETLStep", "apply_transformations"]
