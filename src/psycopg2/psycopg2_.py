import psycopg2


def psy_pg_connect(host, database, user, password):
    """
    Set up a connection to the postgres server

    Parameters
    ----------
    host: str
    database: str
    user: str
    password: str

    Returns
    -------
    tuple
        (connection, cursor)
    """
    connection_string = f"host={host} port=5432 dbname={database} user={user} password={password}"

    connection = psycopg2.connect(connection_string)
    print("PostgreSQL database connected ...")

    '''create a cursor object'''
    cursor = connection.cursor()

    return connection, cursor


def query_to_pandas(query, connection):
    """
    Query result into Pandas dataframe

    Parameters
    ----------
    query: str
    connection: str

    Examples
    --------
    >>> query = '''SELECT * FROM users;'''

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    import pandas as pd
    return pd.io.sql.read_sql_query(query, connection)
