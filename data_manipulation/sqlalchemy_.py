import logging

import sqlalchemy


def create_sqlalchemy_url(
    drivername: str,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port=3306,
) -> sqlalchemy.engine.url.URL:
    """
    Create sqlalchemy url

    Parameters
    ----------
    drivername : str
        firebird+kinterbasdb, mssql+pyodbc, mysql+mysqlconnector, mysql+pymysql, oracle+cx_oracle, postgresql+psycopg2, sapdb+pysapdb, sqlite3, teradata+pytds
    host : str
        Database host address
    dbname : str
        Database name
    user : str
        User used to authenticate
    password : str
        Password used to authenticate
    port : int, optional
        Connection port number, by default 3306

    Returns
    -------
    sqlalchemy.engine.url.URL
        url object
    """
    url = sqlalchemy.engine.url.URL.create(
        drivername=drivername,
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
    )
    return url


def check_connection_valid(
    drivername: str,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port=3306,
) -> sqlalchemy.engine.base.Engine:
    """
    Check mysql connection validity

    Parameters
    ----------
    drivername : str
        firebird+kinterbasdb, mssql+pyodbc, mysql+mysqlconnector, mysql+pymysql, oracle+cx_oracle, postgresql+psycopg2, sapdb+pysapdb, sqlite3, teradata+pytds
    host : str
        Database host address
    dbname : str
        Database name
    user : str
        User used to authenticate
    password : str
        Password used to authenticate
    port : int, optional
        Connection port number, by default 3306

    Returns
    -------
    sqlalchemy.engine.base.Engine
        engine object
    """
    url = create_sqlalchemy_url(
        drivername=drivername,
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
    )
    engine = sqlalchemy.create_engine(url)
    try:
        engine.connect()
        logging.info("check_connection_valid: True")
    except Exception as e:
        logging.error(f"check_connection_valid: False ({e})")
    return engine


if __name__ == "__main__":
    import doctest

    doctest.testmod()
