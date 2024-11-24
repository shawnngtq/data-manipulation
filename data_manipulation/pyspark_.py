import math
import multiprocessing
import os
from typing import Any, Dict, List, Set, Tuple, Union

import pyspark


# CONFIG
def config_spark_local(autoset: bool = True) -> None:
    """Configures Spark for local execution with optimized settings based on system resources.

    Automatically calculates and sets optimal Spark configuration parameters based on:
    - Available CPU cores
    - System memory
    - Executor allocation
    - Memory distribution

    Args:
        autoset (bool, optional): Whether to automatically apply the configuration.
            If False, only prints recommended settings. Defaults to True.

    Examples:
        >>> config_spark_local()
        Here is the current computer specs ...
        executor_per_node: 1
        spark_executor_instances: 1
        total_executor_memory: 30
        spark_executor_memory: 27
        memory_overhead: 3
        spark_default_parallelism: 10
        spark.sql.execution.arrow.pyspark.enabled recommended by Koalas ...
        spark auto-configured ...

    Note:
        Configuration includes:
        - Executor cores and memory
        - Driver cores and memory
        - Memory overhead
        - Default parallelism
        - Shuffle partitions
        - Arrow optimization for PySpark
    """

    def round_down_or_one(x):
        if math.floor(x) == 0:
            return 1
        else:
            return math.floor(x)

    print("Here is the  current computer specs ...")
    vcore_per_node = multiprocessing.cpu_count()
    spark_executor_cores = 5
    number_of_nodes = 1
    total_ram_per_node_gb = (
        os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024.0**3)
    )

    executor_per_node = round_down_or_one((vcore_per_node - 1) / spark_executor_cores)
    spark_executor_instances = round_down_or_one(
        (executor_per_node * number_of_nodes) - 1
    )
    total_executor_memory = round_down_or_one(
        (total_ram_per_node_gb - 1) / executor_per_node
    )
    spark_executor_memory = round_down_or_one(total_executor_memory * 0.9)
    memory_overhead = round_down_or_one(total_executor_memory * 0.1)
    spark_default_parallelism = round_down_or_one(
        spark_executor_instances * spark_executor_cores * 2
    )
    print(f"executor_per_node: {executor_per_node}")
    print(f"spark_executor_instances: {spark_executor_instances}")
    print(f"total_executor_memory: {total_executor_memory}")
    print(f"spark_executor_memory: {spark_executor_memory}")
    print(f"memory_overhead: {memory_overhead}")
    print(f"spark_default_parallelism: {spark_default_parallelism}")

    if autoset:
        spark = (
            pyspark.sql.SparkSession.builder.master("local")
            .config("spark.executor.cores", str(spark_executor_cores))
            .config("spark.driver.cores", str(spark_executor_cores))
            .config("spark.executor.instances", str(spark_executor_instances))
            .config("spark.executor.memory", f"{spark_executor_memory}g")
            .config("spark.driver.memory", f"{spark_executor_memory}g")
            .config("spark.executor.memoryOverhead", f"{memory_overhead}g")
            .config("spark.default.parallelism", str(spark_default_parallelism))
            .config("spark.sql.shuffle.partitions", str(spark_default_parallelism))
            .getOrCreate()
        )

        print("spark.sql.execution.arrow.pyspark.enabled recommended by Koalas ...")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
        spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")
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


# COLUMNS
def add_dummy_columns(
    dataframe: pyspark.sql.DataFrame, columns: List[str], value: str
) -> pyspark.sql.DataFrame:
    """Adds new columns with default values to a Spark DataFrame.

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame
        columns (List[str]): List of column names to add
        value (str): Default value for the new columns

    Returns:
        pyspark.sql.DataFrame: DataFrame with added columns

    Raises:
        TypeError: If arguments are not of correct type
            - dataframe must be a Spark DataFrame
            - columns must be a list
            - value must be a string

    Examples:
        >>> df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
        >>> new_df = add_dummy_columns(df, ["age", "city"], "unknown")
        >>> new_df.show()
        +-----+---+---+------+
        | name| id|age|  city|
        +-----+---+---+------+
        |Alice|  1|unknown|unknown|
        +-----+---+---+------+
    """
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


def column_into_list(dataframe: pyspark.sql.DataFrame, column: str) -> List[Any]:
    """Extracts values from a DataFrame column into a Python list.

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame
        column (str): Name of the column to extract

    Returns:
        List[Any]: List containing all values from the specified column,
            including duplicates

    Raises:
        TypeError: If dataframe is not a Spark DataFrame or column is not a string

    Examples:
        >>> df = spark.createDataFrame([(1,), (2,), (2,)], ["value"])
        >>> column_into_list(df, "value")
        [1, 2, 2]
    """
    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(column, str):
        raise TypeError("Argument must be a str ...")

    if column in dataframe.columns:
        list_ = dataframe.select(column).toPandas()[column].values.tolist()
        return list_


def column_into_set(dataframe: pyspark.sql.DataFrame, column: str) -> Set[Any]:
    """Extracts unique values from a DataFrame column into a Python set.

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame
        column (str): Name of the column to extract

    Returns:
        Set[Any]: Set containing unique values from the specified column

    Raises:
        TypeError: If dataframe is not a Spark DataFrame or column is not a string

    Examples:
        >>> df = spark.createDataFrame([(1,), (2,), (2,)], ["value"])
        >>> column_into_set(df, "value")
        {1, 2}
    """
    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(column, str):
        raise TypeError("Argument must be a str ...")

    set_ = set(column_into_list(dataframe, column))
    return set_


def columns_prefix(
    dataframe: pyspark.sql.DataFrame, prefix: str
) -> pyspark.sql.DataFrame:
    """Adds a prefix to all column names in a DataFrame.

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame
        prefix (str): Prefix to add to column names

    Returns:
        pyspark.sql.DataFrame: DataFrame with renamed columns

    Raises:
        TypeError: If dataframe is not a Spark DataFrame or prefix is not a string

    Examples:
        >>> df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
        >>> new_df = columns_prefix(df, "user_")
        >>> new_df.show()
        +---------+-------+
        |user_name|user_id|
        +---------+-------+
        |    Alice|      1|
        +---------+-------+
    """
    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(prefix, str):
        raise TypeError("Argument must be a str ...")

    df = dataframe
    for column in dataframe.columns:
        if not column.startswith(prefix):
            df = df.withColumnRenamed(column, prefix + column)
    return df


def columns_statistics(
    dataframe: pyspark.sql.DataFrame, n: int = 10
) -> Tuple[List[str], List[str]]:
    """Analyzes column statistics and identifies empty and single-value columns.

    Performs comprehensive analysis of each column including:
    - Value counts
    - Empty value detection
    - Single value detection
    - Basic statistics

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame
        n (int, optional): Number of top values to display for each column.
            Defaults to 10.

    Returns:
        Tuple[List[str], List[str]]: Two lists containing:
            - List of empty column names
            - List of single-value column names

    Raises:
        TypeError: If dataframe is not a Spark DataFrame

    Examples:
        >>> df = spark.createDataFrame([
        ...     ("Alice", None),
        ...     ("Alice", None)
        ... ], ["name", "email"])
        >>> empty_cols, single_cols = columns_statistics(df)
        >>> print(f"Empty columns: {empty_cols}")
        Empty columns: ['email']
        >>> print(f"Single value columns: {single_cols}")
        Single value columns: ['name']
    """
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

            if (
                not df.first()[0]
                or df.first()[0].casefold() == "none"
                or df.first()[0].casefold()
            ):
                empty_columns.append(column)

    print(
        f"There are {len(single_columns)} of single value columns, they are: {single_columns}"
    )
    print(
        f"There are {len(empty_columns)} of null value columns, they are: {empty_columns}"
    )
    return empty_columns, single_columns


# DATAFRAME
def rename(
    dataframe: pyspark.sql.DataFrame, columns: Dict[str, str]
) -> pyspark.sql.DataFrame:
    """Renames multiple columns in a DataFrame using a mapping dictionary.

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame
        columns (Dict[str, str]): Dictionary mapping old column names to new names

    Returns:
        pyspark.sql.DataFrame: DataFrame with renamed columns

    Raises:
        TypeError: If dataframe is not a Spark DataFrame or columns is not a dict

    Examples:
        >>> df = spark.createDataFrame([("Alice", 1)], ["_1", "_2"])
        >>> new_df = rename(df, {"_1": "name", "_2": "id"})
        >>> new_df.show()
        +-----+---+
        | name| id|
        +-----+---+
        |Alice|  1|
        +-----+---+
    """
    import pyspark.sql.functions as F

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(columns, dict):
        raise TypeError("Argument must be a dict ...")

    df = dataframe.select(
        [F.col(c).alias(columns.get(c, c)) for c in dataframe.columns]
    )
    return df


# STATISTICS
def describe(dataframe: pyspark.sql.DataFrame) -> None:
    """Prints comprehensive information about a DataFrame.

    Displays:
    - DataFrame type
    - Number of columns
    - Number of rows
    - Schema information

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame

    Raises:
        TypeError: If dataframe is not a Spark DataFrame

    Examples:
        >>> df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
        >>> describe(df)
        The dataframe: <class 'pyspark.sql.dataframe.DataFrame'>
        Number of columns: 2
        Number of rows: 1
        root
         |-- name: string (nullable = true)
         |-- id: long (nullable = true)
    """
    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    print(f"The dataframe: {type(dataframe)}")
    print(f"Number of columns: {len(dataframe.columns)}")
    print(f"Number of rows: {dataframe.count()}")
    dataframe.printSchema()


def group_count(
    dataframe: pyspark.sql.DataFrame,
    columns: Union[str, List[str]],
    n: Union[int, float] = 10,
) -> pyspark.sql.DataFrame:
    """Performs group by operation and calculates count and percentage for each group.

    Args:
        dataframe (pyspark.sql.DataFrame): Input Spark DataFrame
        columns (Union[str, List[str]]): Column(s) to group by
        n (Union[int, float], optional): Number of top groups to return.
            Use float('inf') for all groups. Defaults to 10.

    Returns:
        pyspark.sql.DataFrame: DataFrame with columns:
            - Group by column(s)
            - count: Count of records in each group
            - percent: Percentage of total records in each group

    Raises:
        TypeError: If arguments are not of correct type

    Examples:
        >>> df = spark.createDataFrame([
        ...     (1, 'A'), (1, 'B'), (2, 'A')
        ... ], ["id", "category"])
        >>> group_count(df, ["id"]).show()
        +---+-----+-------+
        | id|count|percent|
        +---+-----+-------+
        |  1|    2|   66.7|
        |  2|    1|   33.3|
        +---+-----+-------+
    """
    import pyspark.sql.functions as F

    if not isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        raise TypeError("Argument must be a Pyspark dataframe ...")
    if not isinstance(columns, list):
        raise TypeError("Argument must be a list ...")

    df = dataframe.groupBy(columns).count().orderBy("count", ascending=False)
    row_count = dataframe.count()
    df = df.withColumn(
        "percent", F.round(F.udf(lambda x: x * 100 / row_count)("count"), 3)
    )
    if n != float("inf"):
        df = df.limit(n)
    return df


if __name__ == "__main__":
    import doctest

    doctest.testmod()
