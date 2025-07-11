"""
Infraestructura: carga de datos CSV a DataFrame.
"""
import pandas as pd
# import os # Ya no es necesario
from streamlit.runtime.uploaded_file_manager import UploadedFile

def read_csv(archivo_csv: UploadedFile) -> pd.DataFrame:
    """
    Carga un archivo CSV desde un objeto UploadedFile y devuelve un DataFrame.
    Valida que el archivo tiene columnas.
    """
    # Streamlit UploadedFile ya maneja la existencia del archivo temporalmente
    df = pd.read_csv(archivo_csv)
    if df.empty or len(df.columns) == 0:
        raise ValueError("El CSV no tiene columnas o está vacío.")
    return df
