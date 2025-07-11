"""
Módulo de adaptador universal para operaciones de entrada/salida (I/O) en MagicETL.

Este módulo proporciona una interfaz unificada para operaciones de extracción y carga de datos
desde y hacia múltiples fuentes, incluyendo bases de datos y archivos en diversos formatos.
Implementa el patrón Adapter para abstraer las diferencias entre los distintos conectores.

Características principales:
- Extracción de datos desde múltiples fuentes a DataFrames de pandas
- Carga de DataFrames a múltiples destinos
- Detección automática del tipo de fuente/destino basada en la configuración
- Manejo unificado de errores

Este módulo es parte de la arquitectura modular de MagicETL, diseñada siguiendo
los principios SOLID para mejorar la mantenibilidad, testabilidad y extensibilidad.

Ejemplo de uso:
    ```python
    # Extracción desde un archivo CSV
    config_csv = {
        "file_path": "/ruta/a/datos.csv",
        "options": {"sep": ",", "encoding": "utf-8"}
    }
    df = universal_extract_to_df(config_csv)
    
    # Carga a una base de datos PostgreSQL
    db_config = {
        "target_type": "postgresql",
        "connection": {
            "host": "localhost",
            "port": 5432,
            "database": "mi_db",
            "user": "usuario",
            "password": "contraseña"
        },
        "table_name": "mi_tabla",
        "if_exists": "replace"
    }
    universal_write_df(df, db_config)
    ```
"""

import pandas as pd
from typing import Dict, Any

# Assuming 'app' is in PYTHONPATH or discoverable.
# Adjust relative paths if necessary based on your project structure and how it's run.
# For example, if 'app' is the root of your package:
# from .connectors.db_connector import ...
# from .connectors.file_handler import ...
# If running scripts from the root of backend where 'app' is a subdir:
from app.connectors.db_connector import (
    extract_to_df as extract_db_to_df,
    write_df as write_df_to_db,
    UnsupportedDatabaseError as DBUnsupportedError,
    DatabaseConnectionError as DBConnectionError
)
from app.connectors.file_handler import (
    read_file_to_df,
    write_df_to_file,
    UnsupportedFileFormatError as FileUnsupportedError,
    FileProcessingError
)

# Define sets of known types for dispatching
DB_TYPES = {"postgresql", "mysql", "sqlite", "mssql", "oracle", "mongodb"}
FILE_READ_TYPES = {"csv", "json", "xls", "xlsx", "parquet", "xml", "yaml", "yml", "txt"}
FILE_WRITE_TYPES = {"csv", "json", "xls", "xlsx", "parquet", "xml", "yaml", "yml"} # TXT write is often custom

class UniversalIOError(Exception):
    """
    Excepción personalizada para operaciones de I/O universales en MagicETL.
    
    Esta excepción encapsula y proporciona un manejo unificado para todos los errores
    que pueden ocurrir durante las operaciones de extracción y carga de datos,
    independientemente del tipo de fuente o destino (base de datos, archivo, etc.).
    
    Atributos:
        message (str): Mensaje descriptivo del error.
        source (str, opcional): Tipo de fuente o destino donde ocurrió el error.
        original_error (Exception, opcional): Excepción original que causó el error.
    """
    
    def __init__(self, message: str, source: str = None, original_error: Exception = None):
        """
        Inicializa una nueva instancia de UniversalIOError.
        
        Args:
            message: Mensaje descriptivo del error.
            source: Tipo de fuente o destino donde ocurrió el error (ej: 'postgresql', 'csv').
            original_error: Excepción original que causó el error.
        """
        self.message = message
        self.source = source
        self.original_error = original_error
        super().__init__(self.message)

def universal_extract_to_df(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts data from various sources (database or file) into a Pandas DataFrame.
    It dispatches to the appropriate connector based on the 'source_type' in config.
    If 'source_type' is not provided, it tries to infer from 'file_path' for file operations.

    Args:
        config: A dictionary.
                - 'source_type': Explicit type of the source (e.g., 'postgresql', 'csv', 'json').
                - 'file_path': Required for file operations if 'source_type' is a file type or not provided.
                - Other keys depend on the specific source (db connection params, file options).

    Returns:
        A Pandas DataFrame.

    Raises:
        ValueError: If 'source_type' or 'file_path' is insufficient to determine handler.
        UniversalIOError: For errors propagated from underlying handlers or unexpected errors.
    """
    source_type = config.get("source_type") 
    file_path = config.get("file_path")

    try:
        if source_type in DB_TYPES:
            return extract_db_to_df(config)
        elif source_type in FILE_READ_TYPES:
            if not file_path:
                raise ValueError(f"Missing 'file_path' for file source_type '{source_type}'.")
            # Ensure file_format is passed to file_handler if source_type is a file type
            config["file_format"] = source_type 
            return read_file_to_df(config)
        elif file_path: # If source_type is not specified or not a DB type, but file_path is, assume file
            # file_handler.read_file_to_df will attempt to infer format from extension
            return read_file_to_df(config)
        else:
            raise ValueError(
                f"Unsupported or ambiguous source configuration. "
                f"Provide a valid 'source_type' (e.g., {DB_TYPES | FILE_READ_TYPES}) "
                f"and 'file_path' if applicable. Current source_type: '{source_type}'"
            )
    except (DBUnsupportedError, DBConnectionError, FileUnsupportedError, FileProcessingError, ValueError, FileNotFoundError) as e:
        raise UniversalIOError(f"Error during universal extraction: {e}")
    except Exception as e:
        raise UniversalIOError(f"An unexpected error occurred during universal extraction: {e}")

def universal_write_df(df: pd.DataFrame, config: Dict[str, Any]) -> None:
    """
    Writes a Pandas DataFrame to various targets (database or file).
    Dispatches to the appropriate connector based on 'target_type' in config.
    If 'target_type' is not provided, it tries to infer from 'file_path' for file operations.

    Args:
        df: The Pandas DataFrame to write.
        config: A dictionary.
                - 'target_type': Explicit type of the target (e.g., 'postgresql', 'csv', 'json').
                - 'file_path': Required for file operations if 'target_type' is a file type or not provided.
                - Other keys depend on the specific target (db connection/table params, file options).

    Raises:
        ValueError: If 'target_type' or 'file_path' is insufficient to determine handler.
        UniversalIOError: For errors propagated from underlying handlers or unexpected errors.
    """
    target_type = config.get("target_type")
    file_path = config.get("file_path")

    try:
        if target_type in DB_TYPES:
            write_df_to_db(df, config)
        elif target_type in FILE_WRITE_TYPES:
            if not file_path:
                raise ValueError(f"Missing 'file_path' for file target_type '{target_type}'.")
            # Ensure file_format is passed to file_handler if target_type is a file type
            config["file_format"] = target_type
            write_df_to_file(df, config)
        elif file_path: # If target_type is not specified or not a DB type, but file_path is, assume file
            # file_handler.write_df_to_file will attempt to infer format from extension
            write_df_to_file(df, config)
        else:
            raise ValueError(
                f"Unsupported or ambiguous target configuration. "
                f"Provide a valid 'target_type' (e.g., {DB_TYPES | FILE_WRITE_TYPES}) "
                f"and 'file_path' if applicable. Current target_type: '{target_type}'"
            )
    except (DBUnsupportedError, DBConnectionError, FileUnsupportedError, FileProcessingError, ValueError) as e:
        raise UniversalIOError(f"Error during universal write: {e}")
    except Exception as e:
        raise UniversalIOError(f"An unexpected error occurred during universal write: {e}")