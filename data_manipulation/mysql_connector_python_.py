from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Union

from loguru import logger

if TYPE_CHECKING:
    from mysql.connector.connection import MySQLConnection
    from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

    ConnectionType = Union[MySQLConnection, PooledMySQLConnection]
else:
    ConnectionType = Any


class DatabaseError(Exception):
    """Custom exception for database-related errors."""

    pass


def create_connection_pool(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 3306,
    pool_size: int = 5,
) -> Any:
    """Creates a connection pool for MySQL database.

    Args:
        host (str): Database host address.
        dbname (str): Database name to connect to.
        user (str): Username for authentication.
        password (str): Password for authentication.
        port (int, optional): Database port number. Defaults to 3306.
        pool_size (int, optional): Size of the connection pool. Defaults to 5.

    Returns:
        Any: Database connection pool object.

    Raises:
        DatabaseError: If pool creation fails.
    """
    try:
        import mysql.connector
        from mysql.connector import Error as MySQLError
        from mysql.connector import pooling
    except ImportError:
        raise ImportError(
            "mysql-connector-python is required. "
            "Please install it with: pip install mysql-connector-python"
        )

    try:
        pool_config = {
            "pool_name": "mypool",
            "pool_size": pool_size,
            "host": host,
            "database": dbname,
            "user": user,
            "password": password,
            "port": port,
        }
        return pooling.MySQLConnectionPool(**pool_config)
    except MySQLError as e:
        logger.error(f"Failed to create connection pool: {e}")
        raise DatabaseError(f"Connection pool creation failed: {e}")


@contextmanager
def get_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 3306,
    use_pool: bool = False,
    pool: Optional[Any] = None,
) -> Generator[ConnectionType, None, None]:
    """Context manager for database connections.

    Args:
        host (str): Database host address.
        dbname (str): Database name to connect to.
        user (str): Username for authentication.
        password (str): Password for authentication.
        port (int, optional): Database port number. Defaults to 3306.
        use_pool (bool, optional): Whether to use connection pooling. Defaults to False.
        pool (Optional[Any], optional): Existing connection pool.

    Yields:
        ConnectionType: Database connection object.

    Raises:
        DatabaseError: If connection fails.
    """
    try:
        import mysql.connector
        from mysql.connector import Error as MySQLError
    except ImportError:
        raise ImportError(
            "mysql-connector-python is required. "
            "Please install it with: pip install mysql-connector-python"
        )

    connection = None
    try:
        if use_pool and pool:
            connection = pool.get_connection()
        else:
            connection = mysql.connector.MySQLConnection(
                host=host,
                database=dbname,
                user=user,
                password=password,
                port=port,
            )
        logger.info("MySQL database connected...")
        yield connection
    except MySQLError as e:
        logger.error(f"Database connection error: {e}")
        raise DatabaseError(f"Failed to connect to database: {e}")
    finally:
        if (
            connection
            and hasattr(connection, "is_connected")
            and connection.is_connected()
        ):
            connection.close()
            logger.info("MySQL connection closed...")


def execute_query(
    connection: ConnectionType,
    sql_query: str,
    data: Optional[Union[dict, tuple]] = None,
    fetch: bool = False,
) -> Union[Optional[int], List[Dict[str, Any]], None]:
    """Executes a MySQL query with parameters.

    Args:
        connection (ConnectionType): Active MySQL connection.
        sql_query (str): SQL query to execute.
        data (Optional[Union[dict, tuple]], optional): Parameters for the SQL query.
        fetch (bool, optional): Whether to fetch results. Defaults to False.

    Returns:
        Union[Optional[int], List[Dict[str, Any]], None]:
            - For INSERT: Last inserted row ID
            - For SELECT: List of dictionaries containing the results
            - None for other operations or on failure

    Raises:
        DatabaseError: If query execution fails.
    """
    try:
        from mysql.connector import Error as MySQLError
    except ImportError:
        raise ImportError(
            "mysql-connector-python is required. "
            "Please install it with: pip install mysql-connector-python"
        )

    if not hasattr(connection, "is_connected") or not connection.is_connected():
        raise DatabaseError("Database connection is not active")

    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(sql_query, data)

        if sql_query.strip().upper().startswith("SELECT") and fetch:
            return cursor.fetchall()

        if sql_query.strip().upper().startswith("INSERT"):
            connection.commit()
            logger.info("MySQL committed...")
            return cursor.lastrowid

        connection.commit()
        return None

    except MySQLError as e:
        connection.rollback()
        logger.error(f"Query execution error: {e}")
        raise DatabaseError(f"Query execution failed: {e}")
    finally:
        cursor.close()


if __name__ == "__main__":
    import doctest

    doctest.testmod()

    # Usage example:
    """
    # Create a connection pool
    pool = create_connection_pool("localhost", "mydb", "user", "password")

    # Using the connection
    try:
        with get_connection("localhost", "mydb", "user", "password", use_pool=True, pool=pool) as conn:
            # Insert query
            insert_id = execute_query(
                conn,
                "INSERT INTO users (name) VALUES (%(name)s)",
                {"name": "John"}
            )
            
            # Select query
            results = execute_query(
                conn,
                "SELECT * FROM users WHERE id = %(id)s",
                {"id": insert_id},
                fetch=True
            )
    except DatabaseError as e:
        logger.error(f"Database operation failed: {e}")
    """
