"""
Manejador universal de formatos de datos.
Adaptado y mejorado de ai-based-etl-master.
"""

import pandas as pd
import os
import sqlite3
from sqlalchemy import create_engine
import io
import csv
import yaml
import xml.etree.ElementTree as ET
from typing import Dict, Any, Union, List, Optional, BinaryIO
from app.logger import get_logger

logger = get_logger(__name__)

class DataFormatHandler:
    """
    Un manejador universal de formatos de datos que puede:
    1. Leer datos de varios formatos de entrada
    2. Convertir los datos a un DataFrame de pandas
    3. Exportar el DataFrame a varios formatos de salida
    """
    
    @staticmethod
    def detect_format(file_path: str) -> str:
        """
        Detecta el formato de un archivo basado en su extensión.
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            String representando el formato detectado
        """
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        
        format_map = {
            '.csv': 'csv',
            '.tsv': 'csv',  # TSV se maneja con el lector CSV con un delimitador diferente
            '.json': 'json',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.parquet': 'parquet',
            '.pq': 'parquet',
            '.db': 'sqlite',
            '.sqlite': 'sqlite',
            '.xml': 'xml',
            '.yaml': 'yaml',
            '.yml': 'yaml',
            '.txt': 'text'
        }
        
        return format_map.get(ext, 'unknown')
    
    @staticmethod
    def read_file(file_path_or_obj: Union[str, BinaryIO], 
                 format_type: Optional[str] = None,
                 **kwargs) -> pd.DataFrame:
        """
        Lee datos de un archivo o objeto tipo archivo y devuelve un DataFrame de pandas.
        
        Args:
            file_path_or_obj: Ruta al archivo u objeto tipo archivo
            format_type: Formato del archivo (si es None, se auto-detectará)
            **kwargs: Argumentos adicionales para pasar a la función de lectura
            
        Returns:
            DataFrame de pandas conteniendo los datos
        """
        # Si no se proporciona format_type, intentar detectarlo
        if isinstance(file_path_or_obj, str) and format_type is None:
            format_type = DataFormatHandler.detect_format(file_path_or_obj)
            logger.info(f"Formato auto-detectado: {format_type} para archivo: {file_path_or_obj}")
        
        try:
            # Formato CSV (incluyendo TSV con delimitador tab)
            if format_type == 'csv':
                # Verificar si es un archivo TSV
                if isinstance(file_path_or_obj, str) and file_path_or_obj.lower().endswith('.tsv'):
                    return pd.read_csv(file_path_or_obj, delimiter='\t', **kwargs)
                return pd.read_csv(file_path_or_obj, **kwargs)
            
            # Formato JSON
            elif format_type == 'json':
                return pd.read_json(file_path_or_obj, **kwargs)
            
            # Formato Excel
            elif format_type == 'excel':
                return pd.read_excel(file_path_or_obj, **kwargs)
            
            # Formato Parquet
            elif format_type == 'parquet':
                if isinstance(file_path_or_obj, str):
                    return pd.read_parquet(file_path_or_obj, **kwargs)
                else:
                    # Para objetos tipo archivo, necesitamos usar pyarrow
                    import pyarrow.parquet as pq
                    table = pq.read_table(file_path_or_obj)
                    return table.to_pandas()
            
            # Formato SQLite
            elif format_type == 'sqlite':
                if not isinstance(file_path_or_obj, str):
                    raise ValueError("El formato SQLite requiere una ruta de archivo, no un objeto tipo archivo")
                
                query = kwargs.get('query', 'SELECT name FROM sqlite_master WHERE type="table"')
                conn = sqlite3.connect(file_path_or_obj)
                
                if 'table' in kwargs:
                    # Si se solicita una tabla específica, leer esa tabla
                    return pd.read_sql_query(f"SELECT * FROM {kwargs['table']}", conn)
                else:
                    # De lo contrario, devolver la primera tabla
                    tables = pd.read_sql_query(query, conn)
                    if len(tables) > 0:
                        first_table = tables.iloc[0, 0]
                        return pd.read_sql_query(f"SELECT * FROM {first_table}", conn)
                    else:
                        return pd.DataFrame()
            
            # Formato XML
            elif format_type == 'xml':
                if isinstance(file_path_or_obj, str):
                    tree = ET.parse(file_path_or_obj)
                else:
                    tree = ET.parse(file_path_or_obj)
                
                root = tree.getroot()
                data = []
                
                # Análisis XML simple - asume una estructura plana
                for child in root:
                    row = {}
                    for subchild in child:
                        row[subchild.tag] = subchild.text
                    data.append(row)
                
                return pd.DataFrame(data)
            
            # Formato YAML
            elif format_type == 'yaml':
                if isinstance(file_path_or_obj, str):
                    with open(file_path_or_obj, 'r') as f:
                        data = yaml.safe_load(f)
                else:
                    data = yaml.safe_load(file_path_or_obj)
                
                # Convertir a DataFrame - asume una lista de diccionarios
                if isinstance(data, list):
                    return pd.DataFrame(data)
                else:
                    # Si es una estructura anidada, aplanarla
                    return pd.json_normalize(data)
            
            # Formato de texto plano
            elif format_type == 'text':
                if isinstance(file_path_or_obj, str):
                    with open(file_path_or_obj, 'r') as f:
                        lines = f.readlines()
                else:
                    lines = file_path_or_obj.readlines()
                
                # Intentar detectar si es un CSV
                try:
                    dialect = csv.Sniffer().sniff(''.join(lines[:5]))
                    return pd.read_csv(io.StringIO(''.join(lines)), dialect=dialect)
                except Exception:
                    # Si no es un CSV, solo devolver como texto
                    return pd.DataFrame({'text': lines})
            
            # Formato desconocido
            else:
                raise ValueError(f"Formato no soportado: {format_type}")
                
        except Exception as e:
            logger.error(f"Error al leer archivo: {e}")
            raise
    
    @staticmethod
    def write_file(df: pd.DataFrame, 
                  file_path: str, 
                  format_type: Optional[str] = None,
                  **kwargs) -> str:
        """
        Escribe un DataFrame de pandas a un archivo en el formato especificado.
        
        Args:
            df: DataFrame de pandas a escribir
            file_path: Ruta donde escribir el archivo
            format_type: Formato en el que escribir el archivo (si es None, se auto-detectará de la extensión)
            **kwargs: Argumentos adicionales para pasar a la función de escritura
            
        Returns:
            Ruta al archivo escrito
        """
        # Si no se proporciona format_type, intentar detectarlo
        if format_type is None:
            format_type = DataFormatHandler.detect_format(file_path)
            logger.info(f"Formato de salida auto-detectado: {format_type} para archivo: {file_path}")
        
        # Asegurar que el directorio existe
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        try:
            # Formato CSV
            if format_type == 'csv':
                # Verificar si es un archivo TSV
                if file_path.lower().endswith('.tsv'):
                    df.to_csv(file_path, sep='\t', index=False, **kwargs)
                else:
                    df.to_csv(file_path, index=False, **kwargs)
            
            # Formato JSON
            elif format_type == 'json':
                df.to_json(file_path, orient=kwargs.get('orient', 'records'), **kwargs)
            
            # Formato Excel
            elif format_type == 'excel':
                df.to_excel(file_path, index=False, **kwargs)
            
            # Formato Parquet
            elif format_type == 'parquet':
                df.to_parquet(file_path, index=False, **kwargs)
            
            # Formato SQLite
            elif format_type == 'sqlite':
                table_name = kwargs.get('table_name', 'data')
                engine = create_engine(f'sqlite:///{file_path}')
                df.to_sql(table_name, engine, index=False, if_exists='replace')
            
            # Formato XML
            elif format_type == 'xml':
                # Crear un elemento raíz
                root = ET.Element('data')
                
                # Para cada fila, crear un elemento hijo
                for _, row in df.iterrows():
                    row_elem = ET.SubElement(root, 'row')
                    
                    # Para cada columna, crear un subelemento
                    for col, val in row.items():
                        col_elem = ET.SubElement(row_elem, str(col))
                        col_elem.text = str(val)
                
                # Crear el árbol y escribirlo al archivo
                tree = ET.ElementTree(root)
                tree.write(file_path, encoding='utf-8', xml_declaration=True)
            
            # Formato YAML
            elif format_type == 'yaml':
                # Convertir DataFrame a lista de diccionarios
                data = df.to_dict(orient='records')
                
                with open(file_path, 'w') as f:
                    yaml.dump(data, f, default_flow_style=False)
            
            # Formato de texto plano
            elif format_type == 'text':
                # Si hay una sola columna, escribir solo esa columna
                if len(df.columns) == 1:
                    with open(file_path, 'w') as f:
                        for val in df[df.columns[0]]:
                            f.write(f"{val}\n")
                else:
                    # De lo contrario, escribir como CSV
                    df.to_csv(file_path, index=False)
            
            # Formato desconocido
            else:
                raise ValueError(f"Formato de salida no soportado: {format_type}")
            
            logger.info(f"Archivo escrito exitosamente: {file_path}")
            return file_path
                
        except Exception as e:
            logger.error(f"Error al escribir archivo: {e}")
            raise
    
    @staticmethod
    def get_supported_formats() -> Dict[str, List[Dict[str, Any]]]:
        """
        Devuelve una lista de formatos de entrada y salida soportados.
        
        Returns:
            Diccionario con listas de formatos de entrada y salida soportados
        """
        input_formats = [
            {"name": "CSV", "extensions": [".csv"], "description": "Valores separados por comas"},
            {"name": "TSV", "extensions": [".tsv"], "description": "Valores separados por tabulaciones"},
            {"name": "JSON", "extensions": [".json"], "description": "JavaScript Object Notation"},
            {"name": "Excel", "extensions": [".xlsx", ".xls"], "description": "Hojas de cálculo Microsoft Excel"},
            {"name": "Parquet", "extensions": [".parquet", ".pq"], "description": "Formato columnar Apache Parquet"},
            {"name": "SQLite", "extensions": [".db", ".sqlite"], "description": "Base de datos SQLite"},
            {"name": "XML", "extensions": [".xml"], "description": "Extensible Markup Language"},
            {"name": "YAML", "extensions": [".yaml", ".yml"], "description": "YAML Ain't Markup Language"},
            {"name": "Text", "extensions": [".txt"], "description": "Archivo de texto plano"}
        ]
        
        output_formats = [
            {"name": "CSV", "extensions": [".csv"], "description": "Valores separados por comas"},
            {"name": "TSV", "extensions": [".tsv"], "description": "Valores separados por tabulaciones"},
            {"name": "JSON", "extensions": [".json"], "description": "JavaScript Object Notation"},
            {"name": "Excel", "extensions": [".xlsx"], "description": "Hoja de cálculo Microsoft Excel"},
            {"name": "Parquet", "extensions": [".parquet"], "description": "Formato columnar Apache Parquet"},
            {"name": "SQLite", "extensions": [".db"], "description": "Base de datos SQLite"},
            {"name": "XML", "extensions": [".xml"], "description": "Extensible Markup Language"},
            {"name": "YAML", "extensions": [".yaml"], "description": "YAML Ain't Markup Language"},
            {"name": "Text", "extensions": [".txt"], "description": "Archivo de texto plano"}
        ]
        
        return {
            "input_formats": input_formats,
            "output_formats": output_formats
        }
