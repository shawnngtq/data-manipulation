from typing import List, Optional, Union

import pandas as pd
import psycopg2
from loguru import logger


def create_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 5432,
) -> Optional[psycopg2.extensions.connection]:
    """Creates and returns a connection to a PostgreSQL database.

    Args:
        host (str): Database host address (e.g., 'localhost' or IP address)
        dbname (str): Name of the database to connect to
        user (str): Username for database authentication
        password (str): Password for database authentication
        port (int, optional): Database port number. Defaults to 5432.

    Returns:
        Optional[psycopg2.extensions.connection]: Database connection object if successful,
            None if connection fails

    Raises:
        psycopg2.OperationalError: If connection to database fails

    Examples:
        >>> conn = create_connection("localhost", "mydb", "user", "pass")
        >>> isinstance(conn, psycopg2.extensions.connection)
        True
    """
    connection = None
    try:
        connection_string = (
            f"host={host} port={port} dbname={dbname} user={user} password={password}"
        )
        connection = psycopg2.connect(connection_string)
        logger.info("PostgreSQL database connected ...")
    except psycopg2.OperationalError as e:
        logger.error(f"{e}")
    return connection


def execute_query(
    connection: psycopg2.extensions.connection,
    sql_query: str,
    data: Union[dict, tuple, None] = None,
    commit: bool = True,
) -> Optional[int]:
    """Executes a SQL query with optional parameters and transaction control.

    Args:
        connection (psycopg2.extensions.connection): Active PostgreSQL connection
        sql_query (str): SQL query string with placeholders for parameters
        data (Union[dict, tuple, None], optional): Parameters for the SQL query.
            Use dict for named parameters (%(name)s) or tuple for positional parameters (%s).
            Defaults to None.
        commit (bool, optional): Whether to commit the transaction. Defaults to True.

    Returns:
        Optional[int]: Number of rows affected by the query if successful, None if query fails

    Raises:
        psycopg2.OperationalError: If query execution fails

    Examples:
        >>> conn = create_connection("localhost", "mydb", "user", "pass")
        >>> query = "INSERT INTO users (name, age) VALUES (%(name)s, %(age)s)"
        >>> execute_query(conn, query, {"name": "John", "age": 30})
        1  # One row inserted
    """
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, data)
        if commit:
            connection.commit()
            logger.info("PostgreSQL committed ...")
        return cursor.rowcount
    except psycopg2.OperationalError as e:
        logger.error(f"{e}")
        return None


def execute_values(
    connection: psycopg2.extensions.connection,
    table: str,
    columns: List[str],
    values: List[tuple],
    commit: bool = True,
) -> Optional[int]:
    """Efficiently performs bulk insertion of multiple rows into a PostgreSQL table.

    Args:
        connection (psycopg2.extensions.connection): Active PostgreSQL connection
        table (str): Target table name for insertion
        columns (List[str]): List of column names in the target table
        values (List[tuple]): List of value tuples to insert, each tuple corresponding
            to one row of data
        commit (bool, optional): Whether to commit the transaction. Defaults to True.

    Returns:
        Optional[int]: Number of rows successfully inserted if successful, None if operation fails

    Raises:
        psycopg2.OperationalError: If bulk insertion fails

    Examples:
        >>> conn = create_connection("localhost", "mydb", "user", "pass")
        >>> cols = ["name", "age", "email"]
        >>> vals = [
        ...     ("John Doe", 30, "john@example.com"),
        ...     ("Jane Smith", 25, "jane@example.com")
        ... ]
        >>> execute_values(conn, "users", cols, vals)
        2  # Two rows inserted
    """
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
        return connection.cursor().rowcount
    except psycopg2.OperationalError as e:
        logger.error(f"{e}")
        return None


def get_table_columns(
    connection: psycopg2.extensions.connection,
    table: str,
) -> Optional[List[str]]:
    """Retrieves a list of column names from a specified PostgreSQL table.

    Args:
        connection (psycopg2.extensions.connection): Active PostgreSQL connection
        table (str): Name of the table to query

    Returns:
        Optional[List[str]]: List of column names in order of their position in the table,
            None if query fails

    Raises:
        psycopg2.OperationalError: If column information retrieval fails

    Examples:
        >>> conn = create_connection("localhost", "mydb", "user", "pass")
        >>> get_table_columns(conn, "users")
        ['id', 'name', 'email', 'age', 'created_at']
    """
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
        )
        return [row[0] for row in cursor.fetchall()]
    except psycopg2.OperationalError as e:
        logger.error(f"{e}")
        return None


def get_table_schema(
    connection: psycopg2.extensions.connection,
    table: str,
) -> Optional[List[tuple]]:
    """Retrieves detailed schema information for a specified PostgreSQL table.

    Args:
        connection (psycopg2.extensions.connection): Active PostgreSQL connection
        table (str): Name of the table to query

    Returns:
        Optional[List[tuple]]: List of tuples containing column information:
            (column_name, data_type, character_maximum_length, is_nullable)
            Returns None if query fails

    Raises:
        psycopg2.OperationalError: If schema information retrieval fails

    Examples:
        >>> conn = create_connection("localhost", "mydb", "user", "pass")
        >>> schema = get_table_schema(conn, "users")
        >>> for col in schema:
        ...     print(f"Column: {col[0]}, Type: {col[1]}, Max Length: {col[2]}, Nullable: {col[3]}")
        Column: id, Type: integer, Max Length: None, Nullable: NO
        Column: name, Type: character varying, Max Length: 255, Nullable: YES
        Column: created_at, Type: timestamp, Max Length: None, Nullable: NO
    """
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT column_name, data_type, character_maximum_length, is_nullable FROM information_schema.columns WHERE table_name = '{table}'"
        )
        return cursor.fetchall()
    except psycopg2.OperationalError as e:
        logger.error(f"{e}")
        return None


def query_to_pandas(
    connection: psycopg2.extensions.connection,
    sql_query: str,
) -> pd.DataFrame:
    """Executes a SQL query and returns the results as a pandas DataFrame.

    Args:
        connection (psycopg2.extensions.connection): Active PostgreSQL connection
        sql_query (str): SQL query to execute

    Returns:
        pd.DataFrame: Query results converted to a pandas DataFrame

    Raises:
        psycopg2.OperationalError: If query execution fails
        pd.io.sql.DatabaseError: If DataFrame conversion fails

    Examples:
        >>> conn = create_connection("localhost", "mydb", "user", "pass")
        >>> df = query_to_pandas(conn, '''
        ...     SELECT name, age, email
        ...     FROM users
        ...     WHERE age > 25
        ...     ORDER BY age DESC
        ... ''')
        >>> print(df.head())
           name  age           email
        0  John   30  john@example.com
    """

    return pd.read_sql(
        sql=sql_query,
        con=connection,
    )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
