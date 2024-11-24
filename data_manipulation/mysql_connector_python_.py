from typing import Optional, Union

from loguru import logger


def create_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int = 3306,
):
    """Creates a connection to a MySQL database.

    Args:
        host (str): Database host address.
        dbname (str): Database name to connect to.
        user (str): Username for authentication.
        password (str): Password for authentication.
        port (int, optional): Database port number. Defaults to 3306.

    Returns:
        mysql.connector.connection_cext.CMySQLConnection: Database connection object if successful,
            None if connection fails.

    Examples:
        >>> conn = create_connection("localhost", "mydb", "user", "password")
        >>> isinstance(conn, mysql.connector.connection_cext.CMySQLConnection)
        True

    Note:
        Requires mysql-connector-python package.
        Connection failures are logged using loguru logger.
    """
    import mysql.connector

    connection = None
    try:
        connection = mysql.connector.connect(
            host=host,
            database=dbname,
            user=user,
            password=password,
            port=port,
        )
        logger.info("MySQL database connected ...")
    except mysql.connector.errors.Error as e:
        logger.error(f"{e}")
    return connection


def execute_query(
    connection,
    sql_query: str,
    data: Union[dict, tuple],
    commit: bool = True,
) -> Optional[int]:
    """Executes a MySQL query with parameters.

    Args:
        connection (mysql.connector.connection_cext.CMySQLConnection): Active MySQL connection.
        sql_query (str): SQL query to execute.
        data (Union[dict, tuple]): Parameters for the SQL query.
        commit (bool, optional): Whether to commit the transaction. Defaults to True.

    Returns:
        Optional[int]: Last inserted row ID if successful and query was an insert,
            None if query fails or no insert ID is available.

    Examples:
        >>> conn = create_connection(...)
        >>> query = "INSERT INTO users (name) VALUES (%(name)s)"
        >>> execute_query(conn, query, {"name": "John"})
        1  # Returns the new user's ID

    Note:
        Automatically commits transaction if commit=True.
        Query failures are logged using loguru logger.
    """
    import mysql.connector

    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, data)
        if commit:
            connection.commit()
            logger.info("MySQL committed ...")
            id = cursor.lastrowid
            cursor.close()
            return id
    except mysql.connector.errors.Error as e:
        logger.error(f"{e}")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
