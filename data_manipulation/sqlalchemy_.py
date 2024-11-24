import sqlalchemy
from loguru import logger


def create_sqlalchemy_url(
    drivername: str,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 3306,
) -> sqlalchemy.engine.url.URL:
    """Creates a SQLAlchemy URL object for database connection.

    Args:
        drivername (str): Database driver name. Supported options include:
            - 'mysql+mysqlconnector'
            - 'mysql+pymysql'
            - 'postgresql+psycopg2'
            - 'mssql+pyodbc'
            - 'oracle+cx_oracle'
            - 'sqlite3'
        host (str): Database server hostname or IP address
        dbname (str): Name of the target database
        user (str): Database username for authentication
        password (str): Database password for authentication
        port (int, optional): Database server port number. Defaults to 3306.

    Returns:
        sqlalchemy.engine.url.URL: Configured URL object for database connection

    Examples:
        >>> url = create_sqlalchemy_url(
        ...     drivername='postgresql+psycopg2',
        ...     host='localhost',
        ...     dbname='mydb',
        ...     user='admin',
        ...     password='secret',
        ...     port=5432
        ... )
        >>> str(url)
        'postgresql+psycopg2://admin:secret@localhost:5432/mydb'
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


def create_sqlalchemy_engine(
    drivername: str,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 3306,
) -> sqlalchemy.engine.base.Engine:
    """Creates and tests a SQLAlchemy engine for database operations.

    Args:
        drivername (str): Database driver name. Supported options include:
            - 'mysql+mysqlconnector'
            - 'mysql+pymysql'
            - 'postgresql+psycopg2'
            - 'mssql+pyodbc'
            - 'oracle+cx_oracle'
            - 'sqlite3'
        host (str): Database server hostname or IP address
        dbname (str): Name of the target database
        user (str): Database username for authentication
        password (str): Database password for authentication
        port (int, optional): Database server port number. Defaults to 3306.

    Returns:
        sqlalchemy.engine.base.Engine: Configured database engine object

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If engine creation or connection test fails

    Examples:
        >>> engine = create_sqlalchemy_engine(
        ...     drivername='postgresql+psycopg2',
        ...     host='localhost',
        ...     dbname='mydb',
        ...     user='admin',
        ...     password='secret',
        ...     port=5432
        ... )
        # Logs "create_sqlalchemy_engine: True" on success
        # or "create_sqlalchemy_engine: False (error_message)" on failure

    Note:
        The function automatically tests the connection upon creation and logs
        the result using loguru. A successful connection will be logged as info,
        while failures will be logged as errors with the specific exception message.
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
        logger.info("create_sqlalchemy_engine: True")
    except Exception as e:
        logger.error(f"create_sqlalchemy_engine: False ({e})")
    return engine


if __name__ == "__main__":
    import doctest

    doctest.testmod()
