def create_connection(host, dbname, user, password, port=3306):
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
    connection
    """
    import logging
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
        logging.info("MySQL database connected ...")
    except mysql.connector.errors.Error as e:
        logging.info(f"{e}")
    return connection


def execute_query(connection, sql_query, commit=True):
    """
    Execute and commit MySQL query

    Parameters
    ----------
    connection : str
        mysql connection class
    sql_query : str
        SQL query
    commit : bool, optional
        Make database change persistent, by default True

    Returns
    -------
    None
        Just log query commit status
    """
    import logging
    import mysql.connector

    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
        if commit:
            connection.commit()
            logging.info("MySQL committed ...")
    except mysql.connector.errors.Error as e:
        logging.info(f"{e}")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
