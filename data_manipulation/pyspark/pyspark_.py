import pyspark

spark = pyspark.sql.SparkSession.builder.master('local').getOrCreate()


def to_pandas(dataframe, n=10):
    """
    Returns a Pandas dataframe

    Parameters
    ----------
    dataframe: pyspark.sql.dataframe.DataFrame
    n: int / float
        Top n rows

    Examples
    --------
    >>> data = {'int_': [1, 2, 3], 'float_': [-1.0, 0.5, 2.7], 'int_array': [[1, 2], [3, 4, 5], [6, 7, 8, 9]], 'str_array': [[], ['a'], ['a','b']], 'str_rep_array': "[[], ['a'], ['a','b']]", 'str_rep_array2': '"[[], [a], [a,b]]"', 'str_': ['null', '', None]}
    >>> import pandas as pd
    >>> df_pd = pd.DataFrame(data)
    >>> df = spark.createDataFrame(df_pd)

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    if not isinstance(dataframe, (pyspark.sql.dataframe.DataFrame)):
        raise TypeError("Argument must be a Pyspark dataframe ...")

    df_pandas = dataframe.limit(n).toPandas()
    return df_pandas
