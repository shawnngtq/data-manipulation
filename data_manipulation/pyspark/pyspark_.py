def config_spark():
    """
    Configure Spark
    """
    import pyspark
    spark = pyspark.sql.SparkSession.builder.master("local").getOrCreate()


def to_pandas(dataframe, n=10):
    """
    Create pandas dataframe from spark's

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        PySpark dataframe to convert
    n: int / float
        Top n rows

    Examples
    --------
    >>> data = {'int_': [1, 2, 3], 'float_': [-1.0, 0.5, 2.7], 'int_array': [[1, 2], [3, 4, 5], [6, 7, 8, 9]], 'str_array': [[], ['a'], ['a','b']], 'str_rep_array': "[[], ['a'], ['a','b']]", 'str_rep_array2': '"[[], [a], [a,b]]"', 'str_': ['null', '', None]}
    >>> df = spark.createDataFrame(pd.DataFrame(data))
    >>> df_pd = to_pandas(df)
    >>> type(df_pd)
    <class 'pandas.core.frame.DataFrame'>

    Returns
    -------
    pandas.core.frame.DataFrame
        Return pandas dataframe with default 10 rows
    """
    import pyspark

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")

    output = dataframe.limit(n).toPandas()
    return output


def group_count(dataframe, columns, n=10):
    """
    Returns a dataframe group by column(s), sort in descending order, calculate count and percent

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        Spark dataframe
    columns: str / list
        List of column(s) to groupby
    n: int / float
        Top n rows

    Examples
    --------
    >>> data = {'id': [1,2,3,1,2,3,1,2], 'value': [5,2,123,2135,124390,213,2314,96]}
    >>> df = spark.createDataFrame(pd.DataFrame(data))
    >>> group_count(df, ["id"]).show()
    +---+-----+-------+
    | id|count|percent|
    +---+-----+-------+
    |  1|    3|   37.5|
    |  2|    3|   37.5|
    |  3|    2|   25.0|
    +---+-----+-------+
    <BLANKLINE>

    Returns
    -------
    Spark dataframe
        The groupby result
    """
    import pyspark
    import pyspark.sql.functions as F

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(columns, list):
        raise TypeError("Argument must be a list ...")

    output = dataframe.groupBy(columns).count().orderBy("count", ascending=False)
    row_count = dataframe.count()
    output = output.withColumn("percent", F.round(F.udf(lambda x: x * 100 / row_count)("count"), 3))

    if n != float("inf"):
        output = output.limit(n)

    return output


def describe(dataframe):
    """
    Display Spark dataframe information

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        Spark dataframe

    Returns
    -------
    dataframe: pyspark.sql.dataframe.DataFrame
        Similar to pandas dataframe describe()
    """
    import pyspark

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")

    print(f"The dataframe: {type(dataframe)}")
    print(f"Number of columns: {len(dataframe.columns)}")
    print(f"Number of rows: {dataframe.count()}")
    dataframe.printSchema()


def rename(dataframe, columns):
    """
    Rename Spark dataframe column(s)

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    columns: dict
        A dictionary {oldName: newName} of columns to rename

    Returns
    -------
    Spark dataframe
        With renamed column(s)
    """
    import pyspark
    import pyspark.sql.functions as F

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(columns, list):
        raise TypeError("Argument must be a list ...")

    df = dataframe.select([F.col(c).alias(columns.get(c, c)) for c in dataframe.columns])
    return df


def columns_statistics(dataframe, n=10):
    """
    Display Spark dataframe columns' statistics and return 2 lists

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        Spark dataframe
    n: int / float
        Top n rows

    Returns
    -------
    tuple
        ([empty columns], [single columns]). empty list <= single list
    """
    import pyspark

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")

    describe(dataframe)
    empty_columns, single_columns = [], []

    for column in dataframe.columns:
        df = group_count(dataframe=dataframe, columns=column, n=n)
        print(column)
        df.show(n=n)

        if df.count() == 1:
            single_columns.append(column)
            print(f"!!!!! {column} is a candidate to drop !!!!!\n\n")

            if not df.first()[0] or df.first()[0].casefold() == "none" or df.first()[0].casefold():
                empty_columns.append(column)

    print(f"There are {len(single_columns)} of single value columns, they are: {single_columns}")
    print(f"There are {len(empty_columns)} of null value columns, they are: {empty_columns}")
    return empty_columns, single_columns


def column_into_list(dataframe, column):
    """
    Convert a Spark dataframe's column into list

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        Spark dataframe
    column: str
        Column in dataframe

    Returns
    -------
    []
        With possible duplicates
    """
    import pyspark

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(column, str):
        raise TypeError("Argument must be a str ...")

    if column in dataframe.columns:
        list_ = dataframe.select(column).toPandas()[column].values.tolist()
        return list_


def column_into_set(dataframe, column):
    """
    Convert a Spark dataframe's column into set

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        Spark dataframe
    column: str
        Column in dataframe

    Returns
    -------
    {}
        Normal set, no duplicates
    """
    import pyspark

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(column, str):
        raise TypeError("Argument must be a str ...")

    set_ = set(column_into_list(dataframe, column))
    return set_


def columns_prefix(dataframe, prefix):
    """
    Add prefix Spark dataframe's columns

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        Spark dataframe
    prefix: str
        Prefix

    Returns
    -------
    Spark dataframe
        With prefix columns
    """
    import pyspark

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(prefix, str):
        raise TypeError("Argument must be a str ...")

    df = dataframe
    for column in dataframe.columns:
        if not column.startswith(prefix):
            df = df.withColumnRenamed(column, prefix + column)

    return df


def add_dummy_columns(dataframe, columns, value):
    """
    Add dummy column(s) to Spark dataframe

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
        Spark dataframe
    columns: list
        List of column(s)
    value: str
        Default value of the new column(s)

    Returns
    -------
    Spark dataframe
        With additional dummy columns
    """
    import pyspark
    import pyspark.sql.functions as F

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(columns, list):
        raise TypeError("Argument must be a list ...")
    if not isinstance(value, str):
        raise TypeError("Argument must be a str ...")

    df = dataframe
    dummy_columns = set(columns) - set(dataframe.columns)
    for column in dummy_columns:
        df = df.withColumn(column, F.lit(value))

    return df


if __name__ == "__main__":
    import doctest
    import pandas as pd
    import pyspark

    spark = pyspark.sql.SparkSession.builder.master("local").getOrCreate()

    doctest.testmod()
