import logging
from typing import Optional, Union

import pandas as pd
import psycopg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


def create_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port=5432,
) -> psycopg.Connection:
    """
    Return psycopg connection (https://www.psycopg.org/)

    Parameters
    ----------
    host : str
        Database host address
    dbname : str
        Database name
    user : str
        User used to authenticate
    password : str
        Password used to authenticate
    port : int, optional
        Port number, by default 5432

    Returns
    -------
    psycopg.Connection
        Connection object
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
    """
    Execute and commit PostgreSQL query

    Parameters
    ----------
    connection : psycopg.Connection
        _description_
    sql_query : str
        SQL query
    data : Union[dict, tuple]
        Data
    commit : bool, optional
        Make database change persistent, by default True

    Reference
    ---------
    When you insert data key-value with dict type, error
        ProgrammingError: cannot adapt type 'dict' using placeholder '%s' (format: AUTO)
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
    """
    Return Pandas dataframe from result of given SQL query

    Parameters
    ----------
    connection : psycopg.Connection
        _description_
    sql_query : str
        SQL query

    Examples
    --------
    >>> sql_query = '''SELECT * FROM users;'''

    Returns
    -------
    pd.DataFrame
        _description_
    """

    return pd.read_sql(
        sql=sql_query,
        con=connection,
    )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
