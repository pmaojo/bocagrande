import pandas as pd
from pathlib import Path
import yaml # PyYAML
import sqlite3
import csv
import io
from typing import Dict, Any
from sqlalchemy import create_engine

class UnsupportedFileFormatError(Exception):
    """Custom exception for unsupported file formats."""
    pass

class FileProcessingError(Exception):
    """Custom exception for errors during file processing."""
    pass

def read_file_to_df(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Reads data from various file formats into a Pandas DataFrame.
    Auto-detects format from file extension if not specified.

    Args:
        config: A dictionary containing:
            - 'file_path': Absolute path to the input file or a file-like object.
            - 'file_format': Optional. Format of the file (e.g., 'csv', 'json', 'xlsx',
                             'parquet', 'xml', 'yaml', 'txt', 'sqlite', 'tsv').
                             If None, inferred from extension.
            - 'table_name': For SQLite files, the table to read (optional).
            - ... (other format-specific options for pandas.read_* can be passed)

    Returns:
        A Pandas DataFrame containing the extracted data.

    Raises:
        FileNotFoundError: If the file_path does not exist.
        UnsupportedFileFormatError: If the file format is not supported or cannot be inferred.
        FileProcessingError: If any other error occurs during file reading or parsing.
    """
    file_path_obj = config.get("file_path")
    if not file_path_obj:
        raise ValueError("Missing 'file_path' in configuration.")
    
    # Handle both string paths and file-like objects
    is_file_like = hasattr(file_path_obj, 'read')
    
    if is_file_like:
        file_path = file_path_obj  # Use the file-like object directly
        # For file-like objects, format must be specified
        file_format = config.get("file_format")
        if not file_format:
            raise ValueError("'file_format' must be specified when using file-like objects")
    else:
        # It's a path string
        file_path_str = file_path_obj
        file_path = Path(file_path_str)
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"File not found at: {file_path}")

        file_format = config.get("file_format")
        if not file_format:
            file_format = file_path.suffix.lower().lstrip('.')
            # Handle .tsv files specially
            if file_format == 'tsv':
                file_format = 'csv'  # We'll handle TSV as CSV with tab delimiter
    
    # Extract pandas-specific options from config, removing known keys
    pd_options = {k: v for k, v in config.items() if k not in ['file_path', 'file_format', 'table_name']}
    
    # Handle TSV files (CSV with tab delimiter)
    if not is_file_like and isinstance(file_path, Path) and file_path.suffix.lower() == '.tsv':
        if 'delimiter' not in pd_options:
            pd_options['delimiter'] = '\t'

    try:
        if file_format == 'csv':
            return pd.read_csv(file_path, **pd_options)
        elif file_format == 'json':
            # Common 'orient' values: 'records', 'columns', 'values', 'index', 'split'
            # If not specified, pandas attempts to infer. 'lines=True' for JSONL.
            return pd.read_json(file_path, **pd_options)
        elif file_format in ['xls', 'xlsx']:
            return pd.read_excel(file_path, **pd_options)
        elif file_format == 'parquet':
            return pd.read_parquet(file_path, **pd_options)
        elif file_format == 'xml':
            # pd.read_xml requires lxml to be installed.
            return pd.read_xml(file_path, **pd_options)
        elif file_format in ['yaml', 'yml']:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            if isinstance(data, list):
                return pd.DataFrame(data)
            else:
                # Handle non-list YAML structures as a single-row DataFrame or raise error
                # For simplicity, creating a DataFrame from a single dict if applicable
                # This might need refinement based on expected YAML structures
                if isinstance(data, dict):
                     return pd.DataFrame([data])
                raise FileProcessingError("YAML content is not a list of records, cannot directly convert to DataFrame easily.")
        elif file_format == 'sqlite':
            table_name = config.get('table_name')
            if is_file_like:
                raise FileProcessingError("SQLite format requires a file path, not a file-like object")
                
            conn = sqlite3.connect(file_path)
            
            if table_name:
                # If a specific table is requested, read that table
                return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            else:
                # Otherwise, return the first table
                tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
                if len(tables) > 0:
                    first_table = tables.iloc[0, 0]
                    return pd.read_sql_query(f"SELECT * FROM {first_table}", conn)
                else:
                    return pd.DataFrame()
                    
        elif file_format == 'txt':
            # Reads each line as a row in a single-column DataFrame
            if is_file_like:
                lines = file_path.readlines()
                if isinstance(lines[0], bytes):
                    lines = [line.decode('utf-8') for line in lines]
            else:
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                    
            # Try to detect if it's a CSV
            try:
                dialect = csv.Sniffer().sniff(''.join(lines[:5]))
                return pd.read_csv(io.StringIO(''.join(lines)), dialect=dialect)
            except Exception:
                # If not a CSV, just return as text
                return pd.DataFrame(lines, columns=['text_line'])
        else:
            raise UnsupportedFileFormatError(f"Unsupported file format: '{file_format}' for file {file_path}")
    except FileNotFoundError: # Should be caught by pre-check, but as a safeguard
        raise
    except UnsupportedFileFormatError:
        raise
    except Exception as e:
        raise FileProcessingError(f"Error processing file {file_path} (format: {file_format}): {e}")

def write_df_to_file(df: pd.DataFrame, config: Dict[str, Any]) -> None:
    """
    Writes a Pandas DataFrame to various file formats.
    Infers format from file_path extension if 'file_format' is not provided.

    Args:
        df: The Pandas DataFrame to write.
        config: A dictionary containing:
            - 'file_path': Absolute path to the output file.
            - 'file_format': Optional. Format of the file (e.g., 'csv', 'json', 'xlsx',
                             'parquet', 'xml', 'yaml', 'sqlite', 'tsv').
                             If None, inferred from extension.
            - 'index': bool, default False. Whether to write DataFrame index.
            - 'table_name': For SQLite, the table name to write to (default: 'data').
            - ... (other format-specific options for df.to_* can be passed)

    Raises:
        ValueError: If essential configuration keys are missing or DataFrame is empty.
        UnsupportedFileFormatError: If the file format is not supported or cannot be inferred.
        FileProcessingError: If any error occurs during file writing.
    """
    file_path_str = config.get("file_path")
    if not file_path_str:
        raise ValueError("Missing 'file_path' in configuration for writing.")

    if df.empty and config.get("allow_empty_write", False) is not True:
        raise ValueError("Cannot write an empty DataFrame. Set 'allow_empty_write': True in config to override.")

    file_path = Path(file_path_str)
    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)

    file_format = config.get("file_format")
    if not file_format:
        file_format = file_path.suffix.lower().lstrip('.')
    
    # Pandas to_* methods often take 'index' as an argument. Default to False unless specified.
    index = config.get("index", False)
    # Extract pandas-specific options, removing known keys
    pd_options = {k: v for k, v in config.items() if k not in ['file_path', 'file_format', 'index', 'allow_empty_write', 'table_name']}
    
    # Handle TSV files (CSV with tab delimiter)
    if file_path.suffix.lower() == '.tsv':
        if 'sep' not in pd_options:
            pd_options['sep'] = '\t'

    try:
        if file_format == 'csv':
            df.to_csv(file_path, index=index, **pd_options)
        elif file_format == 'json':
            # Common 'orient' values: 'records', 'columns', 'values', 'index', 'split'
            # 'lines=True' for JSONL is often useful. Defaults to 'records' if not in pd_options
            if 'orient' not in pd_options:
                pd_options['orient'] = 'records'
            df.to_json(file_path, index=index, **pd_options)
        elif file_format in ['xls', 'xlsx']:
            df.to_excel(file_path, index=index, **pd_options)
        elif file_format == 'parquet':
            df.to_parquet(file_path, index=index, **pd_options)
        elif file_format == 'xml':
            # df.to_xml requires lxml.
            df.to_xml(file_path, index=index, **pd_options)
        elif file_format in ['yaml', 'yml']:
            with open(file_path, 'w') as f:
                yaml.dump(df.to_dict(orient='records'), f, allow_unicode=True, sort_keys=False)
        elif file_format == 'sqlite':
            table_name = config.get('table_name', 'data')
            engine = create_engine(f'sqlite:///{file_path}')
            df.to_sql(table_name, engine, index=index, if_exists='replace')
            
        elif file_format == 'txt':
            # For TXT, we'll write as CSV with configurable separator
            sep = pd_options.get('sep', '\t')
            header = pd_options.get('header', True)
            df.to_csv(file_path, index=index, sep=sep, header=header)
        else:
            raise UnsupportedFileFormatError(f"Unsupported file format for writing: '{file_format}' for file {file_path}")
    except UnsupportedFileFormatError:
        raise
    except Exception as e:
        raise FileProcessingError(f"Error writing file {file_path} (format: {file_format}): {e}")
