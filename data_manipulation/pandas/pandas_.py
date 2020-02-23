import numpy as np
import pandas as pd

from IPython.display import display


def config_pandas_display():
    """
    Configure Pandas display
    """
    pd.set_option("display.max_columns", 500)
    # pd.set_option("display.max_colwidth", -1)
    pd.set_option("display.max_colwidth", 500)
    # pd.set_option("display.max_rows", 200)
    pd.set_option("display.expand_frame_repr", True)


def add_type_columns(dataframe):
    """
    Returns Pandas dataframe where each columns has their type column right next to it

    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame

    Examples
    --------
    >>> values = [[0, '0', [], {}, None, ""]]
    >>> cols = ["int_", "str_", "list_", "set_", "none_", "_"]
    >>> df = pd.DataFrame(values, columns=cols)
    >>> add_type_columns(df)
       int_      int__type str_      str__type list_      list__type set_       set__type none_          none__type _         __type
    0     0  <class 'int'>    0  <class 'str'>    []  <class 'list'>   {}  <class 'dict'>  None  <class 'NoneType'>    <class 'str'>

    Returns
    -------
    pandas.core.frame.DataFrame
        For every column, add another one with stating the data type
    """
    if not isinstance(dataframe, (pd.DataFrame)):
        raise TypeError("Argument must be a Pandas dataframe ...")

    suffix = "_type"
    for column in dataframe.columns:
        # To prevent creating (1) duplicated column (2) column with ever-increasing suffix [xxx_type, xxx_type_type ...]
        new_col = column + suffix
        if not (new_col in dataframe.columns or column.endswith(suffix)):
            new_col_loc = dataframe.columns.get_loc(column) + 1
            new_col_value = dataframe[column].apply(lambda x: type(x))
            dataframe.insert(new_col_loc, column=new_col, value=new_col_value)
            # dataframe[new_col] = dataframe[column].apply(lambda x: type(x))
    return dataframe


def compare_all_list_items(list_):
    """
    Get dataframe with every combination of 2 list items

    Parameters
    ----------
    list_: list

    Examples
    --------
    >>> input = []
    >>> compare_all_list_items(input)
    Empty DataFrame
    Columns: [item1, item2, item1_eq_item2]
    Index: []

    >>> input = [1, 1, 2, 3]
    >>> compare_all_list_items(input)
      item1 item2  item1_eq_item2
    0     1     1            True
    1     1     2           False
    2     1     3           False
    3     1     2           False
    4     1     3           False
    5     2     3           False

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    if not isinstance(list_, (list)):
        raise TypeError("Argument must be a list ...")

    from itertools import combinations
    result = []
    for a, b in combinations(list_, 2):
        row = {"item1": repr(a), "item2": repr(b), "item1_eq_item2": a == b}
        result.append(row)

    df = pd.DataFrame(result, columns=["item1", "item2", "item1_eq_item2"])
    return df


def columns_to_dictionary(dataframe):
    """
    Create a dictionary {dtype: [column1, column2 ...]} from Pandas dataframe

    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame

    Examples
    --------
    >>> columns_to_dictionary(pd.DataFrame())
    Traceback (most recent call last):
      ...
    ValueError: Argument can't be empty Pandas dataframe ...

    >>> values = [[0, '0', [], {}, None, ""]]
    >>> cols = ["int_", "str_", "list_", "set_", "none_", "_"]
    >>> df = pd.DataFrame(values, columns=cols)
    >>> columns_to_dictionary(df)
    {<class 'numpy.int64'>: ['int_'], <class 'str'>: ['str_', '_'], <class 'list'>: ['list_'], <class 'dict'>: ['set_'], <class 'NoneType'>: ['none_']}

    Returns
    -------
    dict
        {dtype: [column1, column2, ...]}
    """
    if not isinstance(dataframe, (pd.DataFrame)):
        raise TypeError("Argument must be a Pandas dataframe ...")
    if len(dataframe) == 0:
        raise ValueError("Argument can't be a empty Pandas dataframe ...")

    dtype_dict = {}
    for index, value in dataframe.loc[0].iteritems():
        dtype = type(value)
        if dtype not in dtype_dict:
            dtype_dict[dtype] = [index]
        else:
            dtype_dict[dtype].append(index)

    return dtype_dict


def head_tail(dataframe):
    """
    Print the Pandas dataframe first n head and last n tail

    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame
        One Pandas dataframe

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    if not isinstance(dataframe, (pd.DataFrame)):
        raise TypeError("Argument must be a Pandas dataframe ...")

    result = dataframe.head().append(dataframe.tail())
    return result


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
        raise TypeError("Arguments 1 & 2 must be int ...")

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
    dataframe: pandas.core.frame.DataFrame

    chunk_size: int
        The chunk size of each smaller dataframes

    Examples
    --------
    >>> split_pandas_dataframe(pd.DataFrame(), 2)
    [Empty DataFrame
    Columns: []
    Index: []]

    >>> values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    >>> cols = ["test"]
    >>> df = pd.DataFrame(values, columns=cols)
    >>> split_pandas_dataframe(df, 5)
    [   test
    0     1
    1     2
    2     3
    3     4
    4     5,    test
    5     6
    6     7
    7     8
    8     9
    9    10, Empty DataFrame
    Columns: [test]
    Index: []]

    >>> split_pandas_dataframe(df, 4)
    [   test
    0     1
    1     2
    2     3
    3     4,    test
    4     5
    5     6
    6     7
    7     8,    test
    8     9
    9    10]

    Returns
    -------
    list
        [dataframe1, dataframe2, ...]
    """
    result = None
    if not isinstance(dataframe, (pd.DataFrame)):
        raise TypeError("Argument must be a Pandas dataframe ...")

    indices = index_marks(dataframe.shape[0], chunk_size)
    result = np.split(dataframe, indices)
    return result


def to_excel_keep_url_string(filepath, dataframe):
    """
    Save Pandas dataframe as excel, without converting string to urls.
        - Excel auto converts string to urls
        - Excel limit of 65,530 urls per worksheet
        - Excel does not keep url > 255 characters

    Parameters
    ----------
    filepath: str
        Filepath
    dataframe: pandas.core.frame.DataFrame
        Dataframe to export into excel

    Examples
    --------
    >>> values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    >>> cols = ["test"]
    >>> df = pd.DataFrame(values, columns=cols)
    >>> to_excel_keep_url_string("./tmp.xlsx", df)

    Returns
    -------
    None
        Pandas dataframe => excel
    """
    if not isinstance(filepath, (str)):
        raise TypeError("Argument 1 must be a non-empty string ...")

    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument 2 must be a Pandas dataframe ...")

    writer = pd.ExcelWriter(filepath, engine="xlsxwriter", options={"strings_to_urls": False})
    dataframe.to_excel(writer, index=False)
    writer.close()


def compare_dataframes(dataframe, dataframe2):
    """
    Compare the before & after Pandas dataframe going through data transformation (DT)

    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame
        The Pandas dataframe before DT
    dataframe2: pandas.core.frame.DataFrame
        The Pandas dataframe after DT

    Returns
    -------
    None
        Print output comparison between 2 Pandas dataframes
    """
    if not all(isinstance(x, (pd.DataFrame)) for x in [dataframe, dataframe2]):
        raise TypeError("Arguments 1 & 2 must be pandas dataframes ...")

    print("=" * len("Pandas dataframe length"))
    print("Pandas dataframe length")
    print("=" * len("Pandas dataframe length"))
    print(f"df1 (input df)  length: {len(dataframe)}")
    print(f"df2 (output df) length: {len(dataframe)}")
    print(f"dataframe length change: {len(dataframe) == len(dataframe2)}\n")
    print("----------------")
    print("COMPARE COLUMNS")
    print("----------------")
    for column in dataframe.columns:
        pd_df = dataframe[dataframe[column].notnull()]
        pd_df2 = None
        print(f"df1 non-null {column}: {len(pd_df)}")

        try:
            pd_df2 = dataframe2[dataframe2[column].notnull()]
        except KeyError:
            print(f"df2 {column}: does not exist")

        if pd_df2 is not None:
            print(f"df2 non-null {column}: {len(pd_df2)}")

            if len(pd_df) == len(pd_df2):
                print("df1 & df2 has the same length")
            elif len(pd_df2) > len(pd_df):
                print("df2 > df1, check")
            else:
                strings = ["null", "NA"]
                null_count = len(dataframe[dataframe[column].isnull()])
                string_count = len(dataframe[dataframe[column].isin(strings)])
                empty_string_count = len(dataframe[dataframe[column].str.len() == 0])
                non_null_count = len(dataframe) - null_count - string_count - empty_string_count
                print(
                    "df1 - null ({null_count}) - string null ({string_count}) - empty string ({empty_string_count}) = {non_null_count} == df2 ({len(pd_df2)}): {non_null_count == len(pd_df2)}")
        print()


def series_count(series):
    """
    Enhanced Pandas series.value_counts()

    Parameters
    ----------
    series: pd.Series

    Examples
    --------
    >>> values = [1, 1, 2, 3]
    >>> cols = ["test"]
    >>> df = pd.DataFrame(values, columns=cols)
    >>> series_count(df["test"])
       count  percent
    1      2     50.0
    3      1     25.0
    2      1     25.0

    Returns
    -------
    pandas.core.frame.DataFrame
    """
    if not isinstance(series, (pd.Series)):
        raise TypeError("Argument must be a Pandas series ...")

    result = series.value_counts().to_frame(name="count")
    total = result.sum()["count"]
    result["percent"] = result["count"] / total * 100
    return result


def print_dataframe_overview(dataframe, stats=False):
    """
    Print the value_counts() of each column in dataframe

    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame

    Examples
    --------
    >>> print_dataframe_overview(pd.DataFrame())

    >>> data = {"int_": [1, 1, 2], "str_": ["a", "b", "b"]}
    >>> df = pd.DataFrame(data)
    >>> print_dataframe_overview(df)
    ==============================
    int_
    ==============================
    Unique elements: 2
    Null elements: 0
       count    percent
    1      2  66.666667
    2      1  33.333333
    ==============================
    str_
    ==============================
    Unique elements: 2
    Null elements: 0
       count    percent
    b      2  66.666667
    a      1  33.333333

    Returns
    -------
    None
        Print output in console
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a Pandas dataframe ...")

    for column in dataframe.columns:
        print("=" * 30)
        print(column)
        print("=" * 30)
        try:
            print(f"Unique elements: {dataframe[column].nunique()}")
            print(f"Null elements: {len(dataframe[dataframe[column].isnull()])}")
            if stats:
                print(f"Minimum: {min(dataframe[column])}")
                print(f"Maximum: {max(dataframe[column])}")
            display(series_count(dataframe[column]).head(20))
        except:
            print(f"Unable to get value_counts of {column} ...\n")


def useless_columns(dataframe):
    """
    Return useless columns

    Parameters
    ----------
    dataframe: pandas.core.frame.DataFrame

    Returns
    -------
    tuple
        (empty_columns, single_columns)
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a Pandas dataframe ...")

    empty_columns = dataframe.nunique().where(lambda x: x == 0).dropna().index.values.tolist()
    single_columns = dataframe.nunique().where(lambda x: x == 1).dropna().index.values.tolist()
    print(f"Empty columns: {empty_columns}")
    print(f"Single value columns: {single_columns}")
    return empty_columns, single_columns


if __name__ == "__main__":
    import doctest

    doctest.testmod()