def create_connection(host, dbname, user, password, port=5432):
    """
    Return psycopg2 connection

    https://www.psycopg.org/

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
        Connection port number, by default 5432

    Returns
    -------
    connection
    """
    import logging
    import psycopg2

    connection = None
    try:
        connection_string = (
            f"host={host} port={port} dbname={dbname} user={user} password={password}"
        )
        connection = psycopg2.connect(connection_string)
        logging.info("PostgreSQL database connected ...")
    except psycopg2.OperationalError as e:
        logging.info(f"{e}")
    return connection


def execute_sql(connection, sql_query, commit=True):
    """
    Execute and commit PostgreSQL query

    Parameters
    ----------
    connection : str
        psycopg2 connection class
    sql_query : str
        SQL query
    commit : bool, optional
        Make database change persistent, by default True

    Returns
    -------
    None
        Just log sql execute & commit status
    """
    import logging
    import psycopg2

    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
        if commit:
            connection.commit()
            logging.info("PostgreSQL committed ...")
    except psycopg2.OperationalError as e:
        logging.info(f"{e}")


def query_to_pandas(connection, sql_query):
    """
    Return Pandas dataframe from result of given SQL query

    Parameters
    ----------
    connection : str
        psycopg2 connection class
    sql_query : str
        SQL query

    Examples
    --------
    >>> sql_query = '''SELECT * FROM users;'''

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    import pandas as pd

    return pd.io.sql.read_sql_query(
        sql_query,
        connection,
    )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
