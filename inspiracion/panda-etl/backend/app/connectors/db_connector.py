import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from typing import Dict, Any

class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    pass

class UnsupportedDatabaseError(Exception):
    """Custom exception for unsupported database types."""
    pass

def extract_to_df(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts data from various database sources into a Pandas DataFrame.

    Args:
        config: A dictionary containing a 'type' field (e.g., 'postgresql', 'mysql', 
                'sqlite', 'mongodb', 'mssql', 'oracle') and other necessary 
                connection/query parameters.
                
                For SQL-based databases (postgresql, mysql, sqlite, mssql, oracle):
                - 'connection_string': SQLAlchemy connection string.
                - 'query': SQL query to execute.

                For MongoDB:
                - 'connection_string': MongoDB connection URI.
                - 'database_name': Name of the database.
                - 'collection_name': Name of the collection.
                - 'query_filter': Optional dictionary for MongoDB find query filter. Defaults to {}.

    Returns:
        A Pandas DataFrame containing the extracted data.

    Raises:
        UnsupportedDatabaseError: If the database type in config is not supported.
        DatabaseConnectionError: If connection to the database fails or query errors occur.
        ValueError: If essential configuration keys are missing.
    """
    db_type = config.get("type")
    df = pd.DataFrame()

    try:
        if db_type in ["postgresql", "mysql", "sqlite", "mssql", "oracle"]:
            connection_string = config.get("connection_string")
            query = config.get("query")

            if not connection_string:
                raise ValueError(f"Missing 'connection_string' for {db_type} connection.")
            if not query:
                raise ValueError(f"Missing 'query' for {db_type} connection.")

            engine = create_engine(connection_string)
            with engine.connect() as connection:
                df = pd.read_sql_query(text(query), connection)
        
        elif db_type == "mongodb":
            connection_string = config.get("connection_string")
            database_name = config.get("database_name")
            collection_name = config.get("collection_name")
            query_filter = config.get("query_filter", {})

            if not connection_string:
                raise ValueError("Missing 'connection_string' for MongoDB connection.")
            if not database_name:
                raise ValueError("Missing 'database_name' for MongoDB connection.")
            if not collection_name:
                raise ValueError("Missing 'collection_name' for MongoDB connection.")

            client = MongoClient(connection_string)
            db = client[database_name]
            collection = db[collection_name]
            
            cursor = collection.find(query_filter)
            data = list(cursor)
            df = pd.DataFrame(data)
            
            # Attempt to convert _id to string if it exists, for better compatibility
            if "_id" in df.columns:
                try:
                    df["_id"] = df["_id"].astype(str)
                except Exception:
                    # If conversion fails for any reason (e.g. _id is not ObjectId), keep original
                    pass 
            client.close()

        else:
            raise UnsupportedDatabaseError(f"Database type '{db_type}' is not supported.")

    except ImportError as e:
        # This can happen if a driver (e.g., psycopg2, pymysql) is not installed
        raise UnsupportedDatabaseError(f"Driver for database type '{db_type}' not found or import error: {e}")
    except ConnectionRefusedError as e: # More specific for network issues
        raise DatabaseConnectionError(f"Connection refused for {db_type} at {config.get('connection_string', '')}: {e}")
    except Exception as e: 
        # Catch-all for other SQLAlchemy, PyMongo errors (e.g., authentication, operational errors)
        # Consider logging the original exception e for debugging
        # For production, you might want to catch more specific exceptions from 
        # sqlalchemy.exc (e.g., OperationalError, ProgrammingError) and pymongo.errors
        raise DatabaseConnectionError(f"Error during {db_type} operation: {e}")

    return df


def write_df(df: pd.DataFrame, config: Dict[str, Any]) -> None:
    """
    Writes a Pandas DataFrame to various database targets.

    Args:
        df: The Pandas DataFrame to write.
        config: A dictionary containing a 'type' field (e.g., 'postgresql', 'mysql', 
                'sqlite', 'mongodb', 'mssql', 'oracle') and other necessary 
                parameters.
                
                For SQL-based databases (postgresql, mysql, sqlite, mssql, oracle):
                - 'connection_string': SQLAlchemy connection string.
                - 'table_name': Name of the table to write to.
                - 'if_exists': How to behave if the table already exists. 
                               One of 'fail', 'replace', 'append'. Default is 'fail'.
                - 'index': bool, default True. Write DataFrame index as a column.
                           Use index=False to not write the index.
                - 'chunksize': int, optional. Rows will be written in batches of this size at a time.

                For MongoDB:
                - 'connection_string': MongoDB connection URI.
                - 'database_name': Name of the database.
                - 'collection_name': Name of the collection to write to.
                - 'if_exists': How to behave if the collection already exists (mimics pandas to_sql).
                               'fail': If collection is not empty, raise error.
                               'replace': Drop collection, then insert.
                               'append': Insert new documents (may duplicate if not handled by schema/logic).
                               Default is 'fail'.

    Raises:
        UnsupportedDatabaseError: If the database type in config is not supported.
        DatabaseConnectionError: If connection to the database fails or writing errors occur.
        ValueError: If essential configuration keys are missing or DataFrame is empty.
    """
    db_type = config.get("type")

    if df.empty:
        # Consider logging a warning and returning, depending on desired behavior for empty DFs
        # For now, let's raise an error to be explicit.
        raise ValueError("Cannot write an empty DataFrame to the database.")

    try:
        if db_type in ["postgresql", "mysql", "sqlite", "mssql", "oracle"]:
            connection_string = config.get("connection_string")
            table_name = config.get("table_name")
            if_exists = config.get("if_exists", "fail")
            index = config.get("index", True) # pandas default is True
            chunksize = config.get("chunksize")

            if not connection_string:
                raise ValueError(f"Missing 'connection_string' for {db_type} write operation.")
            if not table_name:
                raise ValueError(f"Missing 'table_name' for {db_type} write operation.")
            if if_exists not in ['fail', 'replace', 'append']:
                raise ValueError(f"Invalid value for 'if_exists': {if_exists}. Must be 'fail', 'replace', or 'append'.")

            engine = create_engine(connection_string)
            # Ensure to use a context manager or dispose the engine if it's long-lived elsewhere
            df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=index, chunksize=chunksize)
        
        elif db_type == "mongodb":
            connection_string = config.get("connection_string")
            database_name = config.get("database_name")
            collection_name = config.get("collection_name")
            if_exists = config.get("if_exists", "fail") 

            if not connection_string:
                raise ValueError("Missing 'connection_string' for MongoDB write operation.")
            if not database_name:
                raise ValueError("Missing 'database_name' for MongoDB write operation.")
            if not collection_name:
                raise ValueError("Missing 'collection_name' for MongoDB write operation.")
            if if_exists not in ['fail', 'replace', 'append']:
                raise ValueError(f"Invalid value for 'if_exists' for MongoDB: {if_exists}. Must be 'fail', 'replace', or 'append'.")

            client = MongoClient(connection_string)
            db = client[database_name]
            collection = db[collection_name]
            
            records = df.to_dict(orient='records')

            if not records and if_exists != 'replace': # if df is empty, nothing to insert unless 'replace' (to drop)
                client.close()
                return # Successfully did nothing as per empty DataFrame

            if if_exists == "fail":
                if collection.count_documents({}) > 0:
                    client.close()
                    raise DatabaseConnectionError(f"Collection '{collection_name}' in db '{database_name}' is not empty and if_exists='fail'.")
                if records:
                    collection.insert_many(records, ordered=False)
            elif if_exists == "replace":
                collection.drop()
                if records:
                    collection.insert_many(records, ordered=False)
            elif if_exists == "append":
                if records:
                    collection.insert_many(records, ordered=False)
            
            client.close()

        else:
            raise UnsupportedDatabaseError(f"Database type '{db_type}' is not supported for writing.")

    except ImportError as e:
        raise UnsupportedDatabaseError(f"Driver for database type '{db_type}' not found or import error: {e}")
    except ConnectionRefusedError as e:
        raise DatabaseConnectionError(f"Connection refused for {db_type} at {config.get('connection_string', '')}: {e}")
    except Exception as e:
        # Consider logging the original exception e for debugging
        raise DatabaseConnectionError(f"Error during {db_type} write operation: {e}")

