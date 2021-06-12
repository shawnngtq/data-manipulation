def psy_pg_connect(host, database, username, password):
    """
    Return connection and cursor of psycopg2 postgres server connection

    Parameters
    ----------
    host: str
        Hostname
    database: str
        Database name
    username: str
        Username
    password: str
        Password

    Returns
    -------
    tuple
        (connection, cursor)
    """
    import psycopg2

    connection_string = f"host={host} port=5432 dbname={database} user={username} password={password}"
    connection = psycopg2.connect(connection_string)
    print("PostgreSQL database connected ...")

    cursor = connection.cursor()
    print("Cursor object created ...")
    return connection, cursor


def query_to_pandas(sql_query, connection):
    """
    Return Pandas dataframe from result of given SQL query

    Parameters
    ----------
    sql_query: str
        SQL query
    connection: str
        Connection return by psycopg2

    Examples
    --------
    >>> sql_query = '''SELECT * FROM users;'''

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    import pandas as pd

    return pd.io.sql.read_sql_query(sql_query, connection)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
