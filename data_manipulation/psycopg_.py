from typing import Optional, Union

import pandas as pd
import psycopg
from loguru import logger


def create_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port=5432,
) -> psycopg.Connection:
    """Creates and returns a connection to a PostgreSQL database.

    Args:
        host (str): Database host address (e.g., 'localhost' or IP address)
        dbname (str): Name of the database to connect to
        user (str): Username for database authentication
        password (str): Password for database authentication
        port (int, optional): Database port number. Defaults to 5432.

    Returns:
        psycopg.Connection: Database connection object if successful, None if connection fails

    Raises:
        psycopg.OperationalError: If connection to database fails
    """
    connection = None
    try:
        connection_string = (
            f"host={host} port={port} dbname={dbname} user={user} password={password}"
        )
        connection = psycopg.connect(connection_string)
        logger.info("PostgreSQL database connected ...")
    except psycopg.OperationalError as e:
        logger.error(f"{e}")
    return connection


def execute_sql(
    connection: psycopg.Connection,
    sql_query: str,
    data: Union[dict, tuple],
    commit=True,
) -> None:
    """Executes a SQL query with provided data and optionally commits the transaction.

    Args:
        connection (psycopg.Connection): Active database connection
        sql_query (str): SQL query string with placeholders for data
        data (Union[dict, tuple]): Data to be inserted into the query placeholders.
            Can be either a dictionary for named parameters or a tuple for positional parameters.
        commit (bool, optional): Whether to commit the transaction. Defaults to True.

    Raises:
        psycopg.OperationalError: If query execution fails

    Note:
        When using dictionary data, ensure your SQL query uses named parameters (%(name)s)
        rather than positional parameters (%s) to avoid adaptation errors.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, data)
        if commit:
            connection.commit()
            logger.info("PostgreSQL committed ...")
    except psycopg.OperationalError as e:
        logger.error(f"{e}")


def query_to_pandas(
    connection: psycopg.Connection,
    sql_query: str,
) -> pd.DataFrame:
    """Executes a SQL query and returns the results as a pandas DataFrame.

    Args:
        connection (psycopg.Connection): Active database connection
        sql_query (str): SQL query to execute

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame

    Examples:
        >>> connection = create_connection(host='localhost', dbname='mydb', user='user', password='pass')
        >>> df = query_to_pandas(connection, 'SELECT * FROM users LIMIT 5;')
    """

    return pd.read_sql(
        sql=sql_query,
        con=connection,
    )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
