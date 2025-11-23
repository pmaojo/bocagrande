"""
Infraestructura: carga y exportación de datos a DataFrame.
Consolida lógica de lectura/escritura para CSV, Excel y Parquet.
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import BinaryIO, Union, IO, Optional

import pandas as pd
import pandas.errors
from streamlit.runtime.uploaded_file_manager import UploadedFile

FileType = Union[str, Path, UploadedFile, BinaryIO, IO[str], IO[bytes]]


def read_dataframe(file: FileType, header: int | None | str = 0) -> pd.DataFrame:
    """
    Lee un archivo (CSV, Excel, Parquet) y devuelve un DataFrame.

    Args:
        file: Ruta al archivo o objeto tipo archivo.
        header: Fila a usar como cabecera (0 por defecto). None para sin cabecera.

    Returns:
        pd.DataFrame con los datos.

    Raises:
        ValueError: Si el formato no es soportado o el DataFrame resultante está vacío/sin columnas.
        pd.errors.EmptyDataError: Si el archivo está vacío.
        Exception: Otros errores de lectura.
    """
    # Determinar nombre para inferir formato
    filename = ""
    if isinstance(file, (str, Path)):
        filename = str(file).lower()
    elif hasattr(file, "name"):
        filename = file.name.lower()

    if filename.endswith('.csv'):
        df = pd.read_csv(file, header=header)
    elif filename.endswith('.xlsx') or filename.endswith('.xls'):
        df = pd.read_excel(file, header=header)
    elif filename.endswith('.parquet'):
        df = pd.read_parquet(file)
    else:
        # Fallback o error si no se reconoce extensión
        # Si es un buffer sin nombre, asumimos CSV por defecto?
        # El código original de csv_loader asumía CSV directamente.
        # El de streamlit dependía de la extensión.
        # Si no tiene extensión conocida y es un objeto archivo, intentamos leer como CSV?
        # Mejor ser explícito con el error como en streamlit_app.py
        if not filename:
             # Si no hay nombre, intentamos leer como CSV (caso test con BytesIO sin name)
             try:
                 df = pd.read_csv(file, header=header)
             except pd.errors.EmptyDataError:
                 raise
             except Exception:
                 raise ValueError("No se pudo determinar el formato del archivo y falló la lectura como CSV.")
        else:
            raise ValueError(f"Formato de archivo no soportado: {filename}")

    if df.empty or len(df.columns) == 0:
        raise ValueError("El archivo no tiene columnas o está vacío.")

    return df


def export_dataframe(df: pd.DataFrame, formato: str) -> bytes:
    """
    Exporta un DataFrame al formato especificado.

    Args:
        df: DataFrame a exportar.
        formato: 'csv', 'excel', o 'parquet'.

    Returns:
        bytes: El contenido del archivo exportado.

    Raises:
        ValueError: Si el formato no es soportado.
    """
    buffer = io.BytesIO()
    if formato == 'csv':
        return df.to_csv(index=False).encode('utf-8')
    elif formato == 'excel':
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        return buffer.getvalue()
    elif formato == 'parquet':
        df.to_parquet(buffer, index=False)
        return buffer.getvalue()
    else:
        raise ValueError(f"Formato de exportación no soportado: {formato}")
