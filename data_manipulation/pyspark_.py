def config_spark_local(autoset=True):
    """
    Automatically configure Spark local or provide recommendation if not autoset. Reference https://towardsdatascience.com/basics-of-apache-spark-configuration-settings-ca4faff40d45

    Parameters
    ----------
    autoset : bool
        whether to automatically configure spark

    Examples
    --------
    >>> config_spark_local()
    Here is the  current computer specs ...
    executor_per_node: 1
    spark_executor_instances: 1
    total_executor_memory: 30
    spark_executor_memory: 27
    memory_overhead: 3
    spark_default_parallelism: 10
    spark.sql.execution.arrow.enabled recommended by Koalas ...
    spark auto-configured ...
    config_spark_local exited ...
    >>> config_spark_local(autoset=False)
    Here is the  current computer specs ...
    executor_per_node: 1
    spark_executor_instances: 1
    total_executor_memory: 30
    spark_executor_memory: 27
    memory_overhead: 3
    spark_default_parallelism: 10
    Here is the recommended command to execute:
    <BLANKLINE>
            spark = pyspark.sql.SparkSession.builder.master("local")             .config("spark.executor.cores", "5")             .config("spark.driver.cores", "5")             .config("spark.executor.instances", "1")             .config("spark.executor.memory", "27g")             .config("spark.driver.memory", "27g")             .config("spark.executor.memoryOverhead", "3g")             .config("spark.default.parallelism", "10")             .config("spark.sql.shuffle.partitions", "10")             .getOrCreate()
    <BLANKLINE>
    config_spark_local exited ...

    Returns
    -------
    None
    """
    import multiprocessing
    import math
    import os
    import pyspark

    def round_down_or_one(x):
        if math.floor(x) == 0:
            return 1
        else:
            return math.floor(x)

    print("Here is the  current computer specs ...")
    vcore_per_node = multiprocessing.cpu_count()
    spark_executor_cores = 5
    number_of_nodes = 1
    total_ram_per_node_gb = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024. ** 3)

    executor_per_node = round_down_or_one((vcore_per_node - 1) / spark_executor_cores)
    spark_executor_instances = round_down_or_one((executor_per_node * number_of_nodes) - 1)
    total_executor_memory = round_down_or_one((total_ram_per_node_gb - 1) / executor_per_node)
    spark_executor_memory = round_down_or_one(total_executor_memory * 0.9)
    memory_overhead = round_down_or_one(total_executor_memory * 0.1)
    spark_default_parallelism = round_down_or_one(spark_executor_instances * spark_executor_cores * 2)
    print(f"executor_per_node: {executor_per_node}")
    print(f"spark_executor_instances: {spark_executor_instances}")
    print(f"total_executor_memory: {total_executor_memory}")
    print(f"spark_executor_memory: {spark_executor_memory}")
    print(f"memory_overhead: {memory_overhead}")
    print(f"spark_default_parallelism: {spark_default_parallelism}")

    if autoset:
        global spark
        spark = pyspark.sql.SparkSession.builder.master("local") \
            .config("spark.executor.cores", str(spark_executor_cores)) \
            .config("spark.driver.cores", str(spark_executor_cores)) \
            .config("spark.executor.instances", str(spark_executor_instances)) \
            .config("spark.executor.memory", f"{spark_executor_memory}g") \
            .config("spark.driver.memory", f"{spark_executor_memory}g") \
            .config("spark.executor.memoryOverhead", f"{memory_overhead}g") \
            .config("spark.default.parallelism", str(spark_default_parallelism)) \
            .config("spark.sql.shuffle.partitions", str(spark_default_parallelism)) \
            .getOrCreate()

        print("spark.sql.execution.arrow.enabled recommended by Koalas ...")
        spark.conf.set("spark.sql.execution.arrow.enabled", True)
        spark.conf.get("spark.sql.execution.arrow.enabled")
        print("spark auto-configured ...")
    else:
        print("Here is the recommended command to execute:")
        text = f"""
        spark = pyspark.sql.SparkSession.builder.master("local") \
            .config("spark.executor.cores", "{spark_executor_cores}") \
            .config("spark.driver.cores", "{spark_executor_cores}") \
            .config("spark.executor.instances", "{spark_executor_instances}") \
            .config("spark.executor.memory", "{spark_executor_memory}g") \
            .config("spark.driver.memory", "{spark_executor_memory}g") \
            .config("spark.executor.memoryOverhead", "{memory_overhead}g") \
            .config("spark.default.parallelism", "{spark_default_parallelism}") \
            .config("spark.sql.shuffle.partitions", "{spark_default_parallelism}") \
            .getOrCreate()
        """
        print(text)
    print("config_spark_local exited ...")


def add_dummy_columns(dataframe, columns, value):
    """
    Return dataframe with additional given dummy column(s) and value

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        Spark dataframe
    columns : list
        List of column(s)
    value : str
        Default value of the new column(s)

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
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


def column_into_list(dataframe, column):
    """
    Return list from given dataframe's column, with possible duplicates

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        Spark dataframe
    column : str
        Column in dataframe

    Returns
    -------
    list_ : []
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
    Return normal set from given dataframe's column

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        Spark dataframe
    column : str
        Column in dataframe

    Returns
    -------
    set_ : {}
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
    Return dataframe with renamed columns with given prefix

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        Spark dataframe
    prefix : str
        Prefix

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
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


def columns_statistics(dataframe, n=10):
    """
    Display Spark dataframe columns' statistics and return tuple of empty columns and single columns from given dataframe

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        Spark dataframe
    n : int / float
        Top n rows

    Returns
    -------
    empty_columns : list
        List of empty columns
    single_columns : list
        List of single value columns

    (empty_columns, single_columns)
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


def describe(dataframe):
    """
    Display dataframe information similar to Pandas dataframe describe()

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        Spark dataframe

    Returns
    -------
    None
    """
    import pyspark

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    print(f"The dataframe: {type(dataframe)}")
    print(f"Number of columns: {len(dataframe.columns)}")
    print(f"Number of rows: {dataframe.count()}")
    dataframe.printSchema()


def group_count(dataframe, columns, n=10):
    """
    Returns a dataframe group by column(s), sort in descending order, calculate count and percent

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        Spark dataframe
    columns : str / list
        List of column(s) to groupby
    n : int / float
        Top n rows

    Examples
    --------
    >>> data = {'id': [1, 1, 1, 2, 2, 3], 'value': [5, 2, 5, 2135, 124390, 213]}
    >>> df = spark.createDataFrame(pd.DataFrame(data))
    >>> group_count(df, ["id"]).show()
    +---+-----+-------+
    | id|count|percent|
    +---+-----+-------+
    |  1|    3|   50.0|
    |  2|    2| 33.333|
    |  3|    1| 16.667|
    +---+-----+-------+
    <BLANKLINE>
    >>> group_count(df, ["id", "value"]).show()
    +---+------+-----+-------+
    | id| value|count|percent|
    +---+------+-----+-------+
    |  1|     5|    2| 33.333|
    |  3|   213|    1| 16.667|
    |  2|  2135|    1| 16.667|
    |  1|     2|    1| 16.667|
    |  2|124390|    1| 16.667|
    +---+------+-----+-------+
    <BLANKLINE>

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
    """
    import pyspark
    import pyspark.sql.functions as F

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(columns, list):
        raise TypeError("Argument must be a list ...")

    df = dataframe.groupBy(columns).count().orderBy("count", ascending=False)
    row_count = dataframe.count()
    df = df.withColumn("percent", F.round(F.udf(lambda x: x * 100 / row_count)("count"), 3))
    if n != float("inf"):
        df = df.limit(n)
    return df


def rename(dataframe, columns):
    """
    Return dataframe with new renamed column(s)

    Parameters
    ----------
    dataframe : pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    columns : dict
        A dictionary {oldName: newName} of columns to rename

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
    """
    import pyspark
    import pyspark.sql.functions as F

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(columns, list):
        raise TypeError("Argument must be a list ...")

    df = dataframe.select([F.col(c).alias(columns.get(c, c)) for c in dataframe.columns])
    return df


if __name__ == "__main__":
    import doctest
    import pandas as pd
    import pyspark

    spark = pyspark.sql.SparkSession.builder.master("local").getOrCreate()

    doctest.testmod()
