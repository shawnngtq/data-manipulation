from typing import Optional, Union

from loguru import logger


def create_connection(
    host: str,
    dbname: str,
    user: str,
    password: str,
    port=3306,
):
    """
    Return MySQL connection

    https://github.com/mysql/mysql-connector-python

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
        Connection port number, by default 3306

    Returns
    -------
    mysql.connector.connection_cext.CMySQLConnection
        Connection object
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
    commit=True,
) -> Optional[int]:
    """
    Execute and commit MySQL query

    Parameters
    ----------
    connection : mysql.connector.connection_cext.CMySQLConnection
        mysql connection class
    sql_query : str
        SQL query
    data : Union[dict, tuple]
        _description_
    commit : bool, optional
        Make database change persistent, by default True

    Returns
    -------
    Optional[int]
        Query id should be int
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
