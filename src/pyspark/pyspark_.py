import pyspark
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.master('local').getOrCreate()


def to_pandas(spark_dataframe, n=10):
    """
    Returns a Pandas dataframe

    Parameters
    ----------
    spark_dataframe: pyspark.sql.dataframe.DataFrame
    n: int / float
        Top n rows

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    if not isinstance(spark_dataframe, (pyspark.sql.dataframe.DataFrame)):
        raise TypeError("Argument must be a Pyspark dataframe ...")

    pdDf = spark_dataframe.limit(n).toPandas()
    return pdDf
