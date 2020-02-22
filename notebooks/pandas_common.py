#!/usr/bin/env python
# coding: utf-8

# In[1]:


import ast
import distutils.util
from IPython.display import display, HTML
import itertools
import numpy as np
import pandas as pd


# In[2]:


# PURE PYTHON FUNCTION
def clean_string_literal(string):
    """
    Convert literal string to string
    
    Parameters
    ----------
    string: str
        The column to convert from string literal to string
    
    Returns
    -------
    Cleansed string
    """
    result = None
    if isinstance(string, (str)):
        result = string.strip("'\"")
    
    return result


def convert_string_to_number(string, numeric_dtype=""):
    """
    Convert string number to number (int / float / bool)
    
    Parameters
    ----------
    string: str
        The column to convert from string to number
    
    Returns
    -------
    Number
    """
    result = None
    if isinstance(string, (str)):
        if numeric_dtype == "int":
            result = int(clean_string_literal(string))

        if numeric_dtype == "float":
            result = float(clean_string_literal(string))
        
    return result


def convert_string_to_boolean(string, boolean_dtype="bool"):
    """
    Convert string boolean to boolean (bool / int)
    
    Parameters
    ----------
    string: str
        The column to convert from string to boolean
    
    Notes
    -----
    Do not use eval() as it's unsafe
    
    Returns
    -------
    Boolean or integer
    """
    result = None
    if boolean_dtype == "bool":
        if isinstance(string, (str)):
            result = bool(distutils.util.strtobool(clean_string_literal(string)))

    if boolean_dtype == "int":
        if isinstance(string, (bool)):
            result = int(string)
        if isinstance(string, (str)):
            result = distutils.util.strtobool(clean_string_literal(string))
    
    return result


def convert_string_to_DL(string):
    """
    Convert string dictionary/list to dictionary/list
        
    Parameters
    ----------
    string: str
        The column to convert from string to dictionary/list
    
    Returns
    -------
    Dictionary / list
    """
    result = None
    if isinstance(string, (str)):
        result = ast.literal_eval(clean_string_literal(string))
    
    return result


def print_function(string_func):
    """
    Print string function and its output nicely
    
    Parameters
    ----------
    string_func: str
        String representation of a function
    
    Returns
    -------
    Nicely formatted print output
    """
    print("{0}: {1}".format(string_func, eval(string_func)))





"""PANDAS CONFIGURATION"""
def config_pandas_display():
    """
    Configure Pandas display
    """
    pd.set_option("display.max_columns", 500)
    #pd.set_option("display.max_colwidth", -1)
    pd.set_option("display.max_colwidth", 500)
    #pd.set_option("display.max_rows", 200)
    pd.set_option("display.expand_frame_repr", True)


def reset_pandas_display():
    """
    Reset Pandas display
    """
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.max_colwidth", 50)
    #pd.set_option("display.max_rows", 60)
    pd.set_option("display.expand_frame_repr", True)





"""PANDAS FUNCTION"""
def add_type_columns(dataframe):
    """
    Returns Pandas dataframe where each columns has their type column right next to it
    
    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame
        One Pandas dataframe
    
    Returns
    -------
    A Pandas dataframe
    """    
    if not isinstance(dataframe, (pd.DataFrame)):
        raise ValueError("Argument must be a pandas dataframe")

    suffix = "_type"
    for column in dataframe.columns:
        # To prevent creating (1) duplicated column (2) column with ever-increasing suffix [xxx_type, xxx_type_type ...]
        new_col = column+suffix
        if not (new_col in dataframe.columns or column.endswith(suffix)):
            new_col_loc = dataframe.columns.get_loc(column)+1
            new_col_value = dataframe[column].apply(lambda x: type(x))
            dataframe.insert(new_col_loc, column=new_col, value=new_col_value)
            # dataframe[new_col] = dataframe[column].apply(lambda x: type(x))
    return dataframe


def compare_all_list_items(list_):
    """
    Print every comparison between list's 2 items
    
    Parameters
    ----------
    list_: list
        A list

    Returns
    -------
    A Pandas dataframe
    """
    if not isinstance(list_, (list)):
        raise ValueError("Argument must be a list")

    result = []
    for a, b in itertools.combinations(list_, 2):
        row = {"item1":repr(a), "item2":repr(b), "comparison":a==b}
        result.append(row)

    df = pd.DataFrame(result, columns=["item1","item2","comparison"])
    return df


def head_tail(dataframe):
    """
    Print the Pandas dataframe first n head and last n tail
    
    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame
        One Pandas dataframe
        
    Returns
    -------
    A Pandas dataframe
    """
    if not isinstance(dataframe, (pd.DataFrame)):
        raise ValueError("Argument must be a pandas dataframe")
    
    result = dataframe.head().append(dataframe.tail())
    return result


def columns_to_dictionary(dataframe):
    """
    Create a dictionary {dtype: [columnName1, columnName2 ...]} from Pandas dataframe
    
    Parameters
    ----------
    dataframe: pd.Dataframe
    
    Returns
    -------
    {dtype: [columnName1, columnName2 ...]}
    """
    if not isinstance(dataframe, (pd.DataFrame)):
        raise ValueError("Argument must be a pandas dataframe")
    
    dtype_dict = {}
    for index, value in dataframe.loc[0].iteritems():
        dtype = type(value)
        if dtype not in dtype_dict:
            dtype_dict[dtype] = [index]
        else:
            dtype_dict[dtype].append(index)

    return dtype_dict


def index_marks(n_rows, chunk_size):
    """
    Determine the range given the 
        1. number of rows 
        2. desired chunk size
    
    Parameters
    ----------
    n_rows: int
        The number of rows
    chunk_size: int
        The desire chunk size
    
    Returns
    -------
    Python range
    """
    if not all(isinstance(x, (int)) for x in [n_rows, chunk_size]):
        raise ValueError("Arguments 1 & 2 must be int")
    
    p1 = 1 * chunk_size
    p2 = (n_rows // chunk_size + 1) * chunk_size
    p3 = chunk_size
    result = range(p1, p2, p3)

    return result


def split_pandas_dataframe(dataframe, chunk_size):
    """
    Split Pandas dataframe into dataframes with specific chunk_size
    
    Parameters
    ----------
    dataframe: pd.Dataframe
    chunk_size: int
        The chunk size of each smaller dataframes
    
    Returns
    -------
    Returns list of dataframes
    """    
    indices = index_marks(dataframe.shape[0], chunk_size)
    result = np.split(dataframe, indices)
    
    return result


def excel_keep_url_string(filepath, dataframe):
    """
    Save Pandas dataframe as excel, without converting string to urls.
    - Excel auto converts string to urls
    - Excel limit of 65,530 urls per worksheet
    - Excel does not keep url > 255 characters
    
    Parameters
    ----------
    filepath: str
        File path
    dataframe: pd.Dataframe
        The dataframe to export into excel

    Returns
    -------
    Pandas dataframe => excel
    """
    if not isinstance(filepath, (str)):
        raise ValueError("Argument 1 must be non-empty string")

    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError("Argument 2 must be Pandas dataframe")

    writer = pd.ExcelWriter(filepath, engine="xlsxwriter", options={"strings_to_urls": False})
    dataframe.to_excel(writer, index=False)
    writer.close()


def compare_before_after_dataframes(dataframe, dataframe2):
    """
    Compare the before & after Pandas dataframe going through data transformation (DT)
    
    Parameters
    ----------
    dataframe: pd.Dataframe
        The Pandas dataframe before DT
    dataframe2: pd.Dataframe
        The Pandas dataframe after DT
    
    Returns
    -------
    Output comparison between 2 dataframes
    """
    if not all(isinstance(x, (pd.DataFrame)) for x in [dataframe, dataframe2]):
        raise ValueError("Arguments 1 & 2 must be pandas dataframe")
    
    print("="*len("pandas dataframe length"))
    print("pandas dataframe length")
    print("="*len("pandas dataframe length"))
    print("df1 (input df)  length: {}".format(len(dataframe)))
    print("df2 (output df) length: {}".format(len(dataframe)))
    print("dataframe length change: {}".format(len(dataframe)==len(dataframe2)))
    print()
    
    print('----------------')
    print('COMPARE COLUMNS')
    print('----------------')
    for column in dataframe.columns:
        pd_df = dataframe[dataframe[column].notnull()]
        pd_df2 = None
        print('df1 non-null {}: {}'.format(column, len(pd_df)))

        try:
            pd_df2 = dataframe2[dataframe2[column].notnull()]
        except KeyError:
            print('df2 {}: does not exist'.format(column))

        if pd_df2 is not None:
            print('df2 non-null {}: {}'.format(column, len(pd_df2)))

            if len(pd_df) == len(pd_df2):
                print('df1 & df2 has the same length')
            elif len(pd_df2) > len(pd_df):
                print('df2 > df1, check')
            else:
                strings = ['null', 'NA']
                null_count = len(dataframe[dataframe[column].isnull()])
                string_count = len(dataframe[dataframe[column].isin(strings)])
                empty_string_count = len(dataframe[dataframe[column].str.len()==0])
                non_null_count = len(dataframe) - null_count - string_count - empty_string_count
                print('df1 - null ({}) - string null ({}) - empty string ({}) = {} == df2 ({}): {}'                      .format(null_count, string_count, empty_string_count, non_null_count,
                              len(pd_df2), non_null_count==len(pd_df2)))
        print()
        

def series_count(series):
    """
    Get dataframe's value_counts and percent
    
    Parameters
    ----------
    series: pd.Series
    
    Returns
    -------
    pd.Dataframe
    """
    if not isinstance(series, (pd.Series)):
        raise ValueError("Argument should be pandas series")
    
    result = series.value_counts().to_frame(name="count")
    total = result.sum()["count"]
    result["percent"] = result["count"] / total * 100
    return result


def print_dataframe_overview(dataframe, stats=False):
    """
    Print the value_counts() of each column in dataframe
    
    Parameters
    ----------
    dataframe: pd.Dataframe
    
    Returns
    -------
    Print output in console
    """
    if not isinstance(dataframe, (pd.DataFrame)):
        raise ValueError("Argument should be pandas Dataframe")
        
    for column in dataframe.columns:
        print("="*30)
        print(column)
        print("="*30)
        print("Unique elements: {0}".format(dataframe[column].nunique()))
        #print("NaN: {0}".format(dataframe[column].isnull().sum()))
        #print("Empty string: {0}".format(len(dataframe[dataframe[column] == ""])))
        if stats:
            print("Minimum: {0}".format(min(dataframe[column])))
            print("Maximum: {0}".format(max(dataframe[column])))
        display(val_counts(dataframe[column]).head(20))


# How to hide specific cell in notebook: https://stackoverflow.com/a/48084050
