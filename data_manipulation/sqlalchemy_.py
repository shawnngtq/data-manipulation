from __future__ import annotations

from typing import Any

try:
    import sqlalchemy
    from sqlalchemy.engine.base import Engine
    from sqlalchemy.engine.url import URL
    from sqlalchemy.exc import SQLAlchemyError

    HAS_SQLALCHEMY = True
except ImportError:
    sqlalchemy = None
    HAS_SQLALCHEMY = False

try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""

    pass


def create_sqlalchemy_url(
    drivername: str,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 3306,
    query: dict[str, Any] | None = None,
) -> URL:
    """Creates a SQLAlchemy URL object for database connection.

    Args:
        drivername (str): Database driver name. Supported options include:
            - 'mysql+mysqlconnector'
            - 'mysql+pymysql'
            - 'postgresql+psycopg'
            - 'mssql+pyodbc'
            - 'oracle+cx_oracle'
            - 'sqlite3'
        host (str): Database server hostname or IP address
        dbname (str): Name of the target database
        user (str): Database username for authentication
        password (str): Database password for authentication
        port (int, optional): Database server port number. Defaults to 3306.
        query (Optional[Dict[str, Any]], optional): Additional connection parameters.
            Useful for SSL configuration. Defaults to None.

    Returns:
        sqlalchemy.engine.url.URL: Configured URL object for database connection

    Examples:
        >>> url = create_sqlalchemy_url(
        ...     drivername="postgresql+psycopg",
        ...     host="localhost",
        ...     dbname="mydb",
        ...     user="admin",
        ...     password="secret",
        ...     port=5432,
        ... )
        >>> url.render_as_string(hide_password=False)
        'postgresql+psycopg://admin:secret@localhost:5432/mydb'
    """
    return URL.create(
        drivername=drivername,
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
        query=query or {},
    )


def create_sqlalchemy_engine(
    drivername: str,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 3306,
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout: int = 30,
    connect_timeout: int = 10,
    ssl_ca: str | None = None,
) -> Engine:
    """Creates and tests a SQLAlchemy engine for database operations.

    Args:
        drivername (str): Database driver name. Supported options include:
            - 'mysql+mysqlconnector'
            - 'mysql+pymysql'
            - 'postgresql+psycopg'
            - 'mssql+pyodbc'
            - 'oracle+cx_oracle'
            - 'sqlite3'
        host (str): Database server hostname or IP address
        dbname (str): Name of the target database
        user (str): Database username for authentication
        password (str): Database password for authentication
        port (int, optional): Database server port number. Defaults to 3306.
        pool_size (int, optional): The size of the connection pool. Defaults to 5.
        max_overflow (int, optional): Maximum number of connections above pool_size. Defaults to 10.
        pool_timeout (int, optional): Timeout for getting a connection from pool. Defaults to 30.
        connect_timeout (int, optional): Timeout for database connections. Defaults to 10.
        ssl_ca (Optional[str], optional): Path to SSL CA certificate. Defaults to None.

    Returns:
        sqlalchemy.engine.base.Engine: Configured database engine object

    Raises:
        DatabaseConnectionError: If engine creation or connection test fails

    Examples:
        >>> engine = create_sqlalchemy_engine(  # doctest: +SKIP
        ...     drivername="postgresql+psycopg",
        ...     host="localhost",
        ...     dbname="mydb",
        ...     user="admin",
        ...     password="secret",
        ...     port=5432,
        ... )
        # Logs "create_sqlalchemy_engine: True" on success
        # or "create_sqlalchemy_engine: False (error_message)" on failure

    Note:
        The function automatically tests the connection upon creation and logs
        the result using loguru. A successful connection will be logged as info,
        while failures will be logged as errors with the specific exception message.
    """
    query_params = {}

    # Configure SSL if certificate provided
    if ssl_ca:
        query_params.update({"ssl_ca": ssl_ca, "ssl_verify_cert": "true"})

    # Add connection timeout
    if "mysql" in drivername or "postgresql" in drivername:
        query_params["connect_timeout"] = str(connect_timeout)

    url = create_sqlalchemy_url(
        drivername=drivername,
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        query=query_params,
    )

    engine = sqlalchemy.create_engine(
        url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_pre_ping=True,  # Enable connection health checks
    )

    # Test connection
    try:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        logger.info("Database connection established successfully")
    except SQLAlchemyError as e:
        error_msg = f"Failed to connect to database: {str(e)}"
        logger.error(error_msg)
        engine.dispose()  # Clean up resources
        raise DatabaseConnectionError(error_msg) from e

    return engine
