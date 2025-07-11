"""
Módulo para extraer esquemas de diferentes fuentes de datos.

Este módulo proporciona funcionalidades para extraer y analizar esquemas de datos
de diferentes fuentes como bases de datos, APIs y archivos.
"""

import pandas as pd
import sqlalchemy as sa
import requests
from typing import Dict, List, Any, Optional
from pathlib import Path

from app.logger import get_logger

logger = get_logger(verbose=True)

class SchemaExtractor:
    """
    Clase para extraer esquemas de diferentes fuentes de datos.
    """
    
    def __init__(self):
        """Inicializa el extractor de esquemas."""
        self.supported_file_types = [
            'csv', 'json', 'parquet', 'excel', 'xlsx', 'xls'
        ]
        self.supported_db_types = [
            'postgresql', 'mysql', 'sqlite', 'mssql', 'oracle'
        ]
    
    def extract_from_file(self, file_path: str) -> Dict[str, Any]:
        """
        Extrae el esquema de un archivo.
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            Esquema del archivo como un diccionario
        """
        file_ext = Path(file_path).suffix.lower().lstrip('.')
        
        if file_ext not in self.supported_file_types:
            raise ValueError(f"Tipo de archivo no soportado: {file_ext}")
        
        try:
            if file_ext in ['csv']:
                df = pd.read_csv(file_path, nrows=100)
            elif file_ext in ['json']:
                df = pd.read_json(file_path)
            elif file_ext in ['parquet']:
                df = pd.read_parquet(file_path)
            elif file_ext in ['excel', 'xlsx', 'xls']:
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Tipo de archivo no implementado: {file_ext}")
            
            schema = {
                'source_type': 'file',
                'file_type': file_ext,
                'file_path': file_path,
                'columns': []
            }
            
            for col in df.columns:
                col_info = {
                    'name': col,
                    'type': str(df[col].dtype),
                    'sample_values': df[col].dropna().head(5).tolist() if not df[col].empty else []
                }
                schema['columns'].append(col_info)
            
            schema['row_count'] = len(df)
            
            return schema
            
        except Exception as e:
            logger.log(f"Error extracting schema from file {file_path}: {str(e)}", level=40)
            raise
    
    def extract_from_database(self, connection_string: str, table_name: str) -> Dict[str, Any]:
        """
        Extrae el esquema de una tabla de base de datos.
        
        Args:
            connection_string: Cadena de conexión a la base de datos
            table_name: Nombre de la tabla
            
        Returns:
            Esquema de la tabla como un diccionario
        """
        try:
            engine = sa.create_engine(connection_string)
            inspector = sa.inspect(engine)
            
            if table_name not in inspector.get_table_names():
                raise ValueError(f"Tabla no encontrada: {table_name}")
            
            columns = inspector.get_columns(table_name)
            
            schema = {
                'source_type': 'database',
                'db_type': connection_string.split('://')[0],
                'table_name': table_name,
                'columns': []
            }
            
            for col in columns:
                col_info = {
                    'name': col['name'],
                    'type': str(col['type']),
                    'nullable': col.get('nullable', True)
                }
                schema['columns'].append(col_info)
            
            # Obtener algunas filas de muestra
            with engine.connect() as conn:
                result = conn.execute(sa.text(f"SELECT * FROM {table_name} LIMIT 5"))
                rows = result.fetchall()
                
                if rows:
                    for i, col in enumerate(schema['columns']):
                        col['sample_values'] = [row[i] for row in rows if i < len(row)]
            
            return schema
            
        except Exception as e:
            logger.log(f"Error extracting schema from database table {table_name}: {str(e)}", level=40)
            raise
    
    def extract_from_api(self, url: str, headers: Optional[Dict[str, str]] = None, 
                        params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Extrae el esquema de una API.
        
        Args:
            url: URL de la API
            headers: Cabeceras para la petición HTTP (opcional)
            params: Parámetros para la petición HTTP (opcional)
            
        Returns:
            Esquema de la API como un diccionario
        """
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Convertir a DataFrame para análisis
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if any(isinstance(v, list) for v in data.values()):
                    # Buscar la primera lista en el diccionario
                    for key, value in data.items():
                        if isinstance(value, list) and value:
                            df = pd.DataFrame(value)
                            break
                    else:
                        df = pd.DataFrame([data])
                else:
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Formato de respuesta no soportado: {type(data)}")
            
            schema = {
                'source_type': 'api',
                'url': url,
                'columns': []
            }
            
            for col in df.columns:
                col_info = {
                    'name': col,
                    'type': str(df[col].dtype),
                    'sample_values': df[col].dropna().head(5).tolist() if not df[col].empty else []
                }
                schema['columns'].append(col_info)
            
            schema['row_count'] = len(df)
            
            return schema
            
        except Exception as e:
            logger.log(f"Error extracting schema from API {url}: {str(e)}", level=40)
            raise


def extract_all_schemas(sources: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extrae esquemas de múltiples fuentes de datos.
    
    Args:
        sources: Lista de configuraciones de fuentes de datos
        
    Returns:
        Esquemas combinados como un diccionario
    """
    extractor = SchemaExtractor()
    schemas = []
    
    for source in sources:
        try:
            source_type = source.get('type')
            
            if source_type == 'file':
                schema = extractor.extract_from_file(source.get('file_path'))
            elif source_type == 'database':
                schema = extractor.extract_from_database(
                    source.get('connection_string'),
                    source.get('table_name')
                )
            elif source_type == 'api':
                schema = extractor.extract_from_api(
                    source.get('url'),
                    headers=source.get('headers'),
                    params=source.get('params')
                )
            else:
                raise ValueError(f"Tipo de fuente no soportado: {source_type}")
            
            schemas.append(schema)
            
        except Exception as e:
            logger.log(f"Error extracting schema from source {source}: {str(e)}", level=40)
            raise
    
    return {
        'schemas': schemas,
        'source_count': len(schemas)
    }
