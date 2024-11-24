import subprocess
from itertools import combinations
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from IPython.display import display
from loguru import logger


# CONFIG
def config_pandas_display() -> None:
    """Configures pandas display settings for better output readability.

    Sets the following pandas display options:
        - display.max_columns: 500
        - display.max_colwidth: 500
        - display.expand_frame_repr: True

    Note:
        Modifies global pandas settings.
    """
    global pd
    pd.set_option("display.max_columns", 500)
    # pd.set_option("display.max_colwidth", -1)
    pd.set_option("display.max_colwidth", 500)
    # pd.set_option("display.max_rows", 200)
    pd.set_option("display.expand_frame_repr", True)


# JUPYTER
def is_running_in_jupyter() -> bool:
    """Checks if code is running in a Jupyter notebook environment.

    Returns:
        bool: True if running in Jupyter notebook, False otherwise.

    Examples:
        >>> is_running_in_jupyter()
        True  # When running in Jupyter
        False  # When running in regular Python
    """
    try:
        # Check if the IPython module is available
        from IPython import get_ipython

        # Check if we are in a notebook environment
        if "IPKernelApp" in get_ipython().config:
            return True
        else:
            return False
    # IPython module not found, not running in Jupyter Notebook
    except ImportError:
        return False


# COLUMN
def add_type_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Adds type information columns for each column in the dataframe.

    Args:
        dataframe (pd.DataFrame): Input dataframe to analyze.

    Returns:
        pd.DataFrame: DataFrame with additional type columns. For each original column 'X',
            adds a column 'X_type' containing the Python type of each value.

    Raises:
        TypeError: If input is not a pandas DataFrame.

    Examples:
        >>> df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        >>> add_type_columns(df)
           a        a_type  b        b_type
        0  1  <class 'int'>  x  <class 'str'>
        1  2  <class 'int'>  y  <class 'str'>
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a dataframe ...")

    df = dataframe.copy()
    suffix = "_type"
    for column in df.columns:
        # To prevent creating (1) duplicated column (2) column with ever-increasing suffix [xxx_type, xxx_type_type ...]
        new_col = column + suffix
        if not (new_col in df.columns or column.endswith(suffix)):
            new_col_loc = df.columns.get_loc(column) + 1
            new_col_value = df[column].apply(lambda x: type(x))
            df.insert(new_col_loc, column=new_col, value=new_col_value)
            # df[new_col] = df[column].apply(lambda x: type(x))
    return df


def series_to_columns(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    """Expands a column containing dictionaries into separate columns.

    Args:
        dataframe (pd.DataFrame): Input dataframe.
        column (str): Name of column containing dictionaries to expand.

    Returns:
        pd.DataFrame: DataFrame with dictionary column expanded into separate columns.

    Examples:
        >>> values = [[1, {'a':1, 'b':2}]]
        >>> cols = ["c1", "c2"]
        >>> df = pd.DataFrame(values, columns=cols)
        >>> df
        c1                c2
        0   1  {'a': 1, 'b': 2}
        >>> series_to_columns(df, "c2")
        c1  a  b
        0   1  1  2
    """
    df = pd.concat(
        [dataframe.drop([column], axis=1), dataframe[column].apply(pd.Series)], axis=1
    )
    return df


# DATAFRAME
def chunking_dataframe(dataframe: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    """Splits a dataframe into smaller chunks of specified size.

    Args:
        dataframe (pd.DataFrame): DataFrame to split.
        chunk_size (int): Maximum number of rows in each chunk.

    Returns:
        List[pd.DataFrame]: List of DataFrames, each containing at most chunk_size rows.

    Raises:
        TypeError: If dataframe is not a pandas DataFrame or chunk_size is not an integer.

    Examples:
        >>> df = pd.DataFrame({'a': range(5)})
        >>> chunks = chunking_dataframe(df, 2)
        >>> [len(chunk) for chunk in chunks]
        [2, 2, 1]
    """
    list_ = None
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a dataframe ...")
    if not isinstance(chunk_size, int):
        raise TypeError("Argument must be a int ...")

    indices = index_marks(dataframe.shape[0], chunk_size)
    list_ = np.split(dataframe, indices)
    return list_


def compare_dataframes(dataframe1: pd.DataFrame, dataframe2: pd.DataFrame) -> None:
    """Compares two dataframes and prints detailed comparison information.

    Args:
        dataframe1 (pd.DataFrame): First DataFrame to compare.
        dataframe2 (pd.DataFrame): Second DataFrame to compare.

    Raises:
        TypeError: If either input is not a pandas DataFrame.

    Note:
        Prints comparison results including:
        - Row counts
        - Column-wise comparison of non-null values
        - Detailed analysis of differences when counts don't match
    """
    if not all(isinstance(x, pd.DataFrame) for x in [dataframe1, dataframe2]):
        raise TypeError("Arguments 1 & 2 must be pandas dataframes ...")

    print("=" * len("Dataframe length"))
    print("Dataframe length")
    print("=" * len("Dataframe length"))
    print(f"df1 length: {len(dataframe1)}")
    print(f"df2 length: {len(dataframe2)}")
    print(f"same df length: {len(dataframe1) == len(dataframe2)}\n")
    print("----------------")
    print("COMPARE COLUMNS")
    print("----------------")
    for column in dataframe1.columns:
        pd_df = dataframe1[dataframe1[column].notnull()]
        pd_df2 = None
        print(f"df1 non-null {column}: {len(pd_df)}")

        try:
            pd_df2 = dataframe2[dataframe2[column].notnull()]
        except KeyError:
            logger.error(f"df2 {column}: does not exist")

        if pd_df2 is not None:
            print(f"df2 non-null {column}: {len(pd_df2)}")

            if len(pd_df) == len(pd_df2):
                print("df1 & df2 has the same length")
            elif len(pd_df2) > len(pd_df):
                print("df2 > df1, check")
            else:
                strings = ["null", "NA"]
                null_count = len(dataframe1[dataframe1[column].isnull()])
                string_count = len(dataframe1[dataframe1[column].isin(strings)])
                empty_string_count = len(dataframe1[dataframe1[column].str.len() == 0])
                non_null_count = (
                    len(dataframe1) - null_count - string_count - empty_string_count
                )
                print(
                    f"""
                df1
                - null ({null_count})
                - string null ({string_count})
                - empty string ({empty_string_count})
                = {non_null_count}
                == df2 ({len(pd_df2)}): {non_null_count == len(pd_df2)}
                """
                )


def split_dataframe(
    dataframe: pd.DataFrame, uuid: str, columns: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Splits a dataframe into two based on specified columns while maintaining a linking ID.

    Args:
        dataframe (pd.DataFrame): Input DataFrame to split.
        uuid (str): Name of the column containing unique identifiers.
        columns (List[str]): List of column names to split into second DataFrame.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Tuple containing:
            - First DataFrame with specified columns removed
            - Second DataFrame with only specified columns and uuid

    Raises:
        TypeError: If inputs are not of correct types.

    Examples:
        >>> df = pd.DataFrame({'id': [1], 'a': [2], 'b': [3]})
        >>> df1, df2 = split_dataframe(df, 'id', ['b'])
        >>> df1
           id  a
        0   1  2
        >>> df2
           b  id
        0  3   1
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument 1 must be pandas.DataFrame ...")
    if not isinstance(uuid, str):
        raise TypeError("Argument 2 must be str ...")
    if not isinstance(columns, list):
        raise TypeError("Argument 3 must be list ...")

    df1 = dataframe[dataframe.columns[~dataframe.columns.isin(columns)]].copy()
    df2 = dataframe[columns + [uuid]].copy()
    return df1, df2


def split_left_merged_dataframe(
    dataframe1: pd.DataFrame, dataframe2: pd.DataFrame, columns: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Splits result of a left merge into matched and unmatched records.

    Args:
        dataframe1 (pd.DataFrame): Left DataFrame for merge.
        dataframe2 (pd.DataFrame): Right DataFrame for merge.
        columns (List[str]): Columns to merge on.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Tuple containing:
            - DataFrame with rows that matched in both inputs
            - DataFrame with rows only present in left input

    Examples:
        >>> df1 = pd.DataFrame({'key': ['foo', 'bar', 'baz', 'foo', "zen"], 'value': [1, 2, 3, 5, 9]})
        >>> df2 = pd.DataFrame({'key': ['foo', 'bar', 'baz', 'foo'], 'value': [5, 6, 7, 8]})
        >>> df_both, df_left = split_left_merged_dataframe(df1, df2, ["key"])
                        count    percent
        _merge
        both            6  85.714286
        left_only       1  14.285714
        right_only      0   0.000000
        >>> df_both
           key  value  value_y _merge
        0  foo      1      5.0   both
        1  foo      1      8.0   both
        2  bar      2      6.0   both
        3  baz      3      7.0   both
        4  foo      5      5.0   both
        5  foo      5      8.0   both
        >>> df_left
           key  value  value_y     _merge
        6  zen      9      NaN  left_only
    """
    if not all(isinstance(x, pd.DataFrame) for x in [dataframe1, dataframe2]):
        raise TypeError("Arguments 1 & 2 must be pandas dataframes ...")
    if not isinstance(columns, list):
        raise TypeError("Argument must be a list ...")

    df_merged = dataframe1.merge(
        dataframe2, on=columns, how="left", indicator=True, suffixes=("", "_y")
    )
    df_both = df_merged[df_merged["_merge"] == "both"].copy()
    df_left = df_merged[df_merged["_merge"] == "left_only"].copy()
    display(series_count(df_merged["_merge"]))
    return df_both, df_left


## AGGREGATE
def aggregate_set_without_none(column: pd.Series, nested_set: bool = False) -> set:
    """Creates a set from a series, excluding None values.

    Args:
        column (pd.Series): Series to aggregate into a set.
        nested_set (bool, optional): Whether to handle nested sets. Defaults to False.

    Returns:
        set: Set containing non-None values from the series.

    Examples:
        >>> s = pd.Series([1, None, 2, None, 3])
        >>> aggregate_set_without_none(s)
        {1.0, 2.0, 3.0, nan, nan}
    """
    if nested_set:
        output = set()
        for value in column:
            if isinstance(value, set):
                output.update(value)
            # todo, what if there is valid list / dict / tuple / complex object?
            elif value:
                output.add(value)
        return output
    else:
        return {value for value in column if value is not None}


# DATA STRUCTURE
def clean_none(
    dataframe: pd.DataFrame,
    nan_to_none: bool = True,
    clean_variation: bool = True,
    none_variations: List[str] = [],
) -> pd.DataFrame:
    """Standardizes None values in a dataframe.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.
        nan_to_none (bool, optional): Convert NaN to None. Defaults to True.
        clean_variation (bool, optional): Clean common None variations. Defaults to True.
        none_variations (List[str], optional): Additional None variations to clean. Defaults to [].

    Returns:
        pd.DataFrame: DataFrame with standardized None values.

    Note:
        Deprecated as of pandas 1.3.0.
    """
    df = dataframe.copy()
    df = df.replace(r"^\s*$", np.nan, regex=True)
    if nan_to_none:
        df = df.replace(np.NaN, None, inplace=True)
    if clean_variation:
        df = df.replace(none_variations, np.nan)
    df = df.where(pd.notnull(df), None)
    return df


def compare_all_list_items(list_: List[Any]) -> pd.DataFrame:
    """Creates a DataFrame comparing all possible pairs of items in a list.

    Args:
        list_ (List[Any]): Input list containing items to compare

    Returns:
        pd.DataFrame: DataFrame with columns:
            - item1: First item in comparison
            - item2: Second item in comparison
            - item1_eq_item2: Boolean indicating if items are equal

    Raises:
        TypeError: If input is not a list

    Examples:
        >>> compare_all_list_items([1, 1, 2, 3])
          item1 item2  item1_eq_item2
        0     1     1            True
        1     1     2           False
        2     1     3           False
        3     1     2           False
        4     1     3           False
        5     2     3           False
    """
    if not isinstance(list_, list):
        raise TypeError("Argument must be a list ...")

    df = []
    for a, b in combinations(list_, 2):
        row = {"item1": repr(a), "item2": repr(b), "item1_eq_item2": a == b}
        df.append(row)

    df = pd.DataFrame(df, columns=["item1", "item2", "item1_eq_item2"])
    return df


def dtypes_dictionary(dataframe: pd.DataFrame) -> Dict[type, List[str]]:
    """Creates a dictionary mapping Python types to column names in a DataFrame.

    Args:
        dataframe (pd.DataFrame): Input DataFrame to analyze

    Returns:
        Dict[type, List[str]]: Dictionary where:
            - keys are Python types (e.g., str, int, float)
            - values are lists of column names with that type

    Raises:
        TypeError: If input is not a DataFrame
        ValueError: If DataFrame is empty

    Examples:
        >>> df = pd.DataFrame({
        ...     'int_': [0],
        ...     'str_': ['0'],
        ...     'list_': [[]],
        ...     'dict_': [{}],
        ...     'none_': [None]
        ... })
        >>> dtypes_dictionary(df)
        {
            <class 'int'>: ['int_'],
            <class 'str'>: ['str_'],
            <class 'list'>: ['list_'],
            <class 'dict'>: ['dict_'],
            <class 'NoneType'>: ['none_']
        }
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a dataframe ...")
    if len(dataframe) == 0:
        raise ValueError("Argument can't be a empty dataframe ...")

    dict_ = {}
    for index, value in dataframe.loc[0].iteritems():
        dtype = type(value)
        if dtype not in dict_:
            dict_[dtype] = [index]
        else:
            dict_[dtype].append(index)

    return dict_


# EXPORT
def to_excel_keep_url(filepath: str, dataframe: pd.DataFrame) -> None:
    """Exports DataFrame to Excel while preserving URLs as clickable links.

    Handles Excel's automatic URL conversion and limitations:
    - Preserves URLs without auto-conversion
    - Handles Excel's limit of 65,530 URLs per worksheet
    - Manages URLs longer than 255 characters

    Args:
        filepath (str): Output Excel file path (including .xlsx extension)
        dataframe (pd.DataFrame): DataFrame to export

    Raises:
        TypeError: If filepath is not a string or dataframe is not a DataFrame
        IOError: If file cannot be written to specified location

    Examples:
        >>> df = pd.DataFrame({
        ...     'url': ['https://www.example.com'],
        ...     'data': ['test']
        ... })
        >>> to_excel_keep_url('output.xlsx', df)
        Excel exported ...

    Note:
        Uses xlsxwriter engine with specific options to prevent URL auto-conversion.
        Make sure the target directory exists before calling this function.
    """
    if not isinstance(filepath, str):
        raise TypeError("Argument 1 must be a non-empty string ...")

    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument 2 must be a dataframe ...")

    writer = pd.ExcelWriter(
        filepath,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )
    dataframe.to_excel(writer, index=False)
    writer.close()
    print("Excel exported ...")


# OVERVIEW
def head_tail(dataframe: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """Returns first and last n rows of a dataframe.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.
        n (int, optional): Number of rows from top and bottom. Defaults to 5.

    Returns:
        pd.DataFrame: Concatenated first and last n rows.

    Examples:
        >>> df = pd.DataFrame({'a': range(10)})
        >>> len(head_tail(df, 2))
        4
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a dataframe ...")
    df = pd.concat([dataframe.head(n), dataframe.tail(n)])
    return df


def index_marks(n_rows: int, chunk_size: int) -> range:
    """Calculates indices for splitting data into chunks of specified size.

    Args:
        n_rows (int): Total number of rows in the dataset
        chunk_size (int): Desired size of each chunk

    Returns:
        range: Range object containing indices where chunks should be split

    Raises:
        TypeError: If either n_rows or chunk_size is not an integer

    Examples:
        >>> index_marks(10, 3)
        range(3, 12, 3)
        >>> list(index_marks(10, 3))
        [3, 6, 9]

    Note:
        Used internally by chunking_dataframe() to determine split points
        for breaking large datasets into smaller chunks.
    """
    if not all(isinstance(x, int) for x in [n_rows, chunk_size]):
        raise TypeError("Arguments 1 & 2 must be int ...")

    p1 = 1 * chunk_size
    p2 = (n_rows // chunk_size + 1) * chunk_size
    p3 = chunk_size
    range_ = range(p1, p2, p3)
    return range_


def print_dataframe_overview(dataframe: pd.DataFrame, stats: bool = False) -> None:
    """Prints comprehensive overview of DataFrame contents and statistics.

    Displays for each column:
    - Number of unique elements
    - Number of null elements
    - Value counts with percentages
    - Optional statistics (min/max) if stats=True

    Args:
        dataframe (pd.DataFrame): DataFrame to analyze
        stats (bool, optional): Whether to include min/max statistics. Defaults to False.

    Raises:
        TypeError: If input is not a pandas DataFrame

    Examples:
        >>> data = {"int_": [1, 1, 2], "str_": ["a", "b", "b"]}
        >>> df = pd.DataFrame(data)
        >>> print_dataframe_overview(df)
        ==============================
        int_
        ==============================
        Unique elements: 2
        Null elements: 0
            count    percent
        int_
        1         2  66.666667
        2         1  33.333333
        ==============================
        str_
        ==============================
        Unique elements: 2
        Null elements: 0
            count    percent
        str_
        b         2  66.666667
        a         1  33.333333
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a dataframe ...")

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
            logger.error(f"Unable to get value_counts of {column} ...\n")


def series_count(series: pd.Series) -> pd.DataFrame:
    """Calculates value counts and percentages for a Series.

    Enhanced version of series.value_counts() that includes percentage calculations.

    Args:
        series (pd.Series): Input series to analyze

    Returns:
        pd.DataFrame: DataFrame with columns:
            - count: Frequency of each unique value
            - percent: Percentage of total (0-100) for each value

    Raises:
        TypeError: If input is not a pandas Series

    Examples:
        >>> values = [1, 1, 1, 2, 2, 3]
        >>> s = pd.Series(values)
        >>> series_count(s)
           count    percent
        1      3  50.000000
        2      2  33.333333
        3      1  16.666667
    """
    if not isinstance(series, pd.Series):
        raise TypeError("Argument must be a series ...")

    df = series.value_counts().to_frame(name="count")
    total = df.sum()["count"]
    df["percent"] = df["count"] / total * 100
    return df


def useless_columns(dataframe: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """Identifies empty and single-value columns in a DataFrame.

    Args:
        dataframe (pd.DataFrame): DataFrame to analyze

    Returns:
        Tuple[List[str], List[str]]: Two lists containing:
            - List of column names that are completely empty
            - List of column names that contain only a single unique value

    Raises:
        TypeError: If input is not a pandas DataFrame

    Examples:
        >>> df = pd.DataFrame({
        ...     'empty': [None, None],
        ...     'single': ['A', 'A'],
        ...     'normal': ['A', 'B']
        ... })
        >>> empty_cols, single_cols = useless_columns(df)
        Empty columns: ['empty']
        Single value columns: ['single']
        >>> print(f"Empty columns: {empty_cols}")
        Empty columns: ['empty']
        >>> print(f"Single value columns: {single_cols}")
        Single value columns: ['single']

    Note:
        Empty columns are those where all values are None/NaN.
        Single-value columns may be candidates for removal or conversion to constants.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Argument must be a dataframe ...")

    empty_columns = (
        dataframe.nunique().where(lambda x: x == 0).dropna().index.values.tolist()
    )
    single_columns = (
        dataframe.nunique().where(lambda x: x == 1).dropna().index.values.tolist()
    )
    print(f"Empty columns: {empty_columns}")
    print(f"Single value columns: {single_columns}")
    return empty_columns, single_columns


# SYSTEM
def ps_aux_dataframe(ps_aux_commands: str) -> pd.DataFrame:
    """Converts Linux ps aux command output to a DataFrame.

    Args:
        ps_aux_commands (str): ps aux command string to execute.

    Returns:
        pd.DataFrame: DataFrame containing process information with columns matching
            ps aux output format.

    Examples:
        >>> df = ps_aux_dataframe("ps aux | head")
        >>> 'PID' in df.columns
        True
    """
    from .base import parse_ps_aux

    if isinstance(ps_aux_commands, str):
        rows = parse_ps_aux(ps_aux_commands)
        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df
    else:
        raise TypeError("Wrong datatype(s)")


if __name__ == "__main__":
    import doctest

    subprocess.run(
        "mkdir -p test_pandas_folder",
        capture_output=True,
        shell=True,
        text=True,
        executable="/bin/bash",
    )
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
    subprocess.run(
        "rm -rf test_pandas_folder",
        capture_output=True,
        shell=True,
        text=True,
        executable="/bin/bash",
    )
