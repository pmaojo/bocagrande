"""
Infraestructura: carga de datos CSV a DataFrame.
"""
from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import BinaryIO, Union, IO

from streamlit.runtime.uploaded_file_manager import UploadedFile

FileType = Union[str, Path, UploadedFile, BinaryIO, IO[str]]


def read_csv(archivo_csv: FileType) -> pd.DataFrame:
    """Return a DataFrame from ``archivo_csv`` validating it has columns."""
    df = pd.read_csv(archivo_csv)
    if df.empty or len(df.columns) == 0:
        raise ValueError("El CSV no tiene columnas o está vacío.")
    return df
