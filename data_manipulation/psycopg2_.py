import logging
from typing import Optional, Union

import pandas as pd
import psycopg2


def create_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port=5432,
) -> psycopg2.extensions.connection:
    """
    Return psycopg2 connection (https://www.psycopg.org/)

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
    psycopg2.extensions.connection
        Connection object
    """
    connection = None
    try:
        connection_string = (
            f"host={host} port={port} dbname={dbname} user={user} password={password}"
        )
        connection = psycopg2.connect(connection_string)
        logging.info("PostgreSQL database connected ...")
    except psycopg2.OperationalError as e:
        logging.error(f"{e}")
    return connection


def execute_sql(
    connection: psycopg2.extensions.connection,
    sql_query: str,
    data: Union[dict, tuple],
    commit=True,
) -> None:
    """
    Execute and commit PostgreSQL query

    Parameters
    ----------
    connection : psycopg2.extensions.connection
        _description_
    sql_query : str
        SQL query
    data : Union[dict, tuple]
        Data
    commit : bool, optional
        Make database change persistent, by default True
    """
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, data)
        if commit:
            connection.commit()
            logging.info("PostgreSQL committed ...")
    except psycopg2.OperationalError as e:
        logging.error(f"{e}")


def query_to_pandas(
    connection: psycopg2.extensions.connection,
    sql_query: str,
) -> pd.DataFrame:
    """
    Return Pandas dataframe from result of given SQL query

    Parameters
    ----------
    connection : psycopg2.extensions.connection
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
