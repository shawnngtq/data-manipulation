from typing import Any, List, Optional, Union

import pandas as pd
from loguru import logger

try:
    import psycopg
    import psycopg2
    import psycopg2.extras

    PSYCOPG_AVAILABLE = True
    PSYCOPG2_AVAILABLE = True
except ImportError as e:
    if "psycopg" in str(e):
        PSYCOPG_AVAILABLE = False
    if "psycopg2" in str(e):
        PSYCOPG2_AVAILABLE = False

# Type alias for connection objects
PostgresConnection = Union["psycopg.Connection", "psycopg2.extensions.connection"]


def create_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 5432,
    use_psycopg2: bool = False,
) -> Optional[PostgresConnection]:
    """Creates and returns a connection to a PostgreSQL database.

    Args:
        host (str): Database host address
        dbname (str): Name of the database
        user (str): Username for authentication
        password (str): Password for authentication
        port (int, optional): Database port number. Defaults to 5432.
        use_psycopg2 (bool, optional): Whether to use psycopg2 instead of psycopg. Defaults to False.

    Returns:
        Optional[PostgresConnection]: Database connection object if successful, None if fails
    """
    connection = None
    connection_string = (
        f"host={host} port={port} dbname={dbname} user={user} password={password}"
    )

    try:
        if use_psycopg2 and PSYCOPG2_AVAILABLE:
            connection = psycopg2.connect(connection_string)
        elif PSYCOPG_AVAILABLE:
            connection = psycopg.connect(connection_string)
        else:
            raise ImportError("Neither psycopg nor psycopg2 is available")
        logger.info("PostgreSQL database connected ...")
    except Exception as e:
        logger.error(f"Connection error: {e}")

    return connection


def execute_query(
    connection: PostgresConnection,
    sql_query: str,
    data: Union[dict, tuple, None] = None,
    commit: bool = True,
) -> Optional[int]:
    """Executes a SQL query with optional parameters.

    Args:
        connection: Active PostgreSQL connection
        sql_query (str): SQL query string with placeholders
        data: Parameters for the SQL query
        commit (bool, optional): Whether to commit the transaction. Defaults to True.

    Returns:
        Optional[int]: Number of rows affected if successful, None if fails
    """
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, data)
        affected_rows = cursor.rowcount

        if commit:
            connection.commit()
            logger.info("PostgreSQL committed ...")

        return affected_rows
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        return None


def execute_values(
    connection: PostgresConnection,
    table: str,
    columns: List[str],
    values: List[tuple],
    commit: bool = True,
) -> Optional[int]:
    """Efficiently performs bulk insertion of multiple rows.

    Args:
        connection: Active PostgreSQL connection
        table (str): Target table name
        columns (List[str]): List of column names
        values (List[tuple]): List of value tuples to insert
        commit (bool, optional): Whether to commit. Defaults to True.

    Returns:
        Optional[int]: Number of rows inserted if successful, None if fails
    """
    if not isinstance(connection, psycopg2.extensions.connection):
        logger.warning("execute_values is only supported with psycopg2")
        return None

    try:
        psycopg2.extras.execute_values(
            cursor=connection.cursor(),
            sql=f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
            argslist=values,
            template=None,
        )
        if commit:
            connection.commit()
            logger.info("PostgreSQL committed ...")
        return len(values)
    except Exception as e:
        logger.error(f"Bulk insertion error: {e}")
        return None


def get_table_info(
    connection: PostgresConnection, table: str, info_type: str = "columns"
) -> Optional[Union[List[str], List[tuple]]]:
    """Retrieves table information (columns or schema).

    Args:
        connection: Active PostgreSQL connection
        table (str): Name of the table
        info_type (str): Type of information to retrieve ('columns' or 'schema')

    Returns:
        Optional[Union[List[str], List[tuple]]]: Requested table information or None if fails
    """
    try:
        cursor = connection.cursor()
        if info_type == "columns":
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table}'"
            )
            return [row[0] for row in cursor.fetchall()]
        elif info_type == "schema":
            cursor.execute(
                "SELECT column_name, data_type, character_maximum_length, is_nullable "
                "FROM information_schema.columns "
                f"WHERE table_name = '{table}'"
            )
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Table info retrieval error: {e}")
        return None


def query_to_pandas(
    connection: PostgresConnection,
    sql_query: str,
) -> pd.DataFrame:
    """Executes a SQL query and returns results as a pandas DataFrame.

    Args:
        connection: Active PostgreSQL connection
        sql_query (str): SQL query to execute

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame
    """
    return pd.read_sql(sql=sql_query, con=connection)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
