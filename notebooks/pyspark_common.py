
# coding: utf-8

# In[1]:


import pyspark
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.master('local').getOrCreate()


# In[2]:


# def config_spark():
#     '''
#     Configure Spark
#     '''
#     import pyspark
#     import pyspark.sql.functions as F
#     import pyspark.sql.types as T
#     spark = pyspark.sql.SparkSession.builder.master('local').getOrCreate()
    

def to_pandas(sparkDf, n=10):
    '''
    Return a Pandas dataframe
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    n: int / float
        Top n rows
    
    Return
    ------
    A Pandas dataframe
    '''
    pdDf = sparkDf.limit(n).toPandas()
    return pdDf


def group_count_percent(sparkDf, cols, n=10, dfType='pandas'):
    '''
    Return a Pandas dataframe group by column(s), sort in descending order, calculate count and percent
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    cols: str / list
        Accepts a (list of) string(s) of column(s)
    n: int / float
        Top n rows
        
    Return
    ------
    A Pandas dataframe
    '''
    df = sparkDf.groupBy(cols).count().orderBy('count', ascending=False)
    rowCount = sparkDf.count()
    
    if n == float('inf'):
        df = df.withColumn('percent', F.round(F.udf(lambda x: x*100/rowCount)('count'), 3))
    else:
        df = df.withColumn('percent', F.round(F.udf(lambda x: x*100/rowCount)('count'), 3)).limit(n)
    
    if dfType == 'pandas':
        pdDf = df.toPandas()
        return pdDf
    
    if dfType == 'spark':
        return df


def info(sparkDf):
    '''
    Display Spark dataframe information
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
    '''
    print('The dataframe: {0}'.format(type(sparkDf)))
    print('Number of columns: {0}'.format(len(sparkDf.columns)))
    print('Number of rows: {0}'.format(sparkDf.count()))
    sparkDf.printSchema()


def rename_columns(sparkDf, cols):
    '''
    Rename Spark dataframe column(s)
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    cols: dict
        A dictionary {oldName: newName} of columns to rename
    
    Return
    ------
    A Spark dataframe
    '''
    df = sparkDf.select([F.col(c).alias(cols.get(c,c)) for c in sparkDf.columns])
    return df


def columns_statistics(sparkDf, n=10):
    '''
    Display Spark dataframe columns' statistics and return 2 lists

    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    n: int / float
        Top n rows
    
    Return
    ------
    Lists of null-value and single-value columns. null-value list <= single-value list
    '''
    info(sparkDf)
    nullValueCols, singleValueCols = [], []
    
    for column in sparkDf.columns:
        df = group_count_percent(sparkDf=sparkDf, cols=column, n=n, dfType='Spark')
        print(column)
        df.show(n=n)
        
        if df.count() == 1:
            singleValueCols.append(column)
            print('!!!!! {0} is a candidate to drop !!!!!\n\n'.format(column))
        
            if not df.first()[0] or df.first()[0].casefold() == 'none' or df.first()[0].casefold():
                nullValueCols.append(column)
        
    print('There are {0} of single value columns, they are: {1}'.format(len(singleValueCols), singleValueCols))
    print('There are {0} of null value columns, they are: {1}'.format(len(nullValueCols), nullValueCols))
    return nullValueCols, singleValueCols


def column_into_list(sparkDf, singleCol):
    '''
    Convert a Spark dataframe's column into list
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    singleCol: str
        One column in sparkDf
    
    Return
    ------
    A list
    '''
    if col in sparkDf.columns:
        LIST = sparkDf.select(singleCol).toPandas()[col].values.tolist()
        return LIST


def column_into_set(sparkDf, singleCol):
    '''
    Convert a Spark dataframe's column into set
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    singleCol: str
        One column in sparkDf
    
    Return
    ------
    A set
    '''
    SET = set(column_into_list(sparkDf, singleCol))
    return SET


def prefix_to_columns(sparkDf, prefix):
    '''
    Add prefix Spark dataframe's columns
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    prefix: str
        Prefix
        
    Return
    ------
    A Spark dataframe
    '''
    df = sparkDf
    for column in sparkDf.columns:
        if not column.startswith(prefix):
            df = df.withColumnRenamed(column, prefix+column)
    
    return df


def add_dummy_columns(sparkDf, cols, value):
    '''
    Add dummy column(s) to Spark dataframe
    
    Parameters
    ----------
    sparkDf: pyspark.sql.dataframe.DataFrame
        One Spark dataframe
    cols: list
        List of columns
    value: str
        Default value of the new column(s)
    
    Return
    ------
    A Spark dataframe
    '''
    df = sparkDf
    dummyCols = set(cols) - set(sparkDf.columns)
    for column in dummyCols:
        df = df.withColumn(column, F.lit(value))
    
    return df
