import psycopg2


def psy_pg_connect(host, database, username, password):
    """
    Set up a connection to the postgres server

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
    connection_string = f"host={host} port=5432 dbname={database} username={username} password={password}"
    connection = psycopg2.connect(connection_string)
    print("PostgreSQL database connected ...")

    cursor = connection.cursor()
    print("Cursor object created ...")
    return connection, cursor


def query_to_pandas(sql_query, connection):
    """
    Query result into Pandas dataframe

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
        SQL results in Pandas dataframe
    """
    import pandas as pd
    return pd.io.sql.read_sql_query(sql_query, connection)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
