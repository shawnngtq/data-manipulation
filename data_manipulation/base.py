import itertools
import os
import re
import subprocess
from typing import List

from loguru import logger


# DATA STRUCTURE
def clean_string(
    string: str, remove_parenthesis: bool = False, remove_brackets: bool = False
) -> str:
    """Cleans and standardizes input string.

    Args:
        string (str): String to clean.
        remove_parenthesis (bool, optional): Whether to remove content within parentheses. Defaults to False.
        remove_brackets (bool, optional): Whether to remove content within square brackets. Defaults to False.

    Returns:
        str: Uppercase cleaned string with standardized spacing.

    Examples:
        >>> clean_string(" sHawn  tesT ")
        'SHAWN TEST'
        >>> clean_string("shawn ( te  st )")
        'SHAWN ( TE ST )'
        >>> clean_string("shawn ( te  st )", remove_parenthesis=True)
        'SHAWN'
        >>> clean_string("shawn [ te  st ]", remove_brackets=True)
        'SHAWN'
    """

    if remove_parenthesis:
        string = re.sub(r"\(.*\)", "", string)
    if remove_brackets:
        string = re.sub(r"\[.*\]", "", string)

    string = string.strip().upper()
    string = " ".join(string.split())
    return string


def get_string_case_combination(str_: str) -> list:
    """Generates all possible case combinations of a string.

    Args:
        str_ (str): Input string to generate combinations for.

    Returns:
        list: List of all possible case combinations.

    Examples:
        >>> get_string_case_combination("abc")
        ['ABC', 'ABc', 'AbC', 'Abc', 'aBC', 'aBc', 'abC', 'abc']
    """

    return list(map("".join, itertools.product(*zip(str_.upper(), str_.lower()))))


def get_none_variation() -> list:
    """Returns a list of common variations of None/null values.

    Returns:
        list: List containing None and various string representations of null values.

    Examples:
        >>> get_none_variation()
        [None, 'NONE', 'NONe', 'NOnE', 'NOne', 'NoNE', 'NoNe', 'NonE', 'None', 'nONE', 'nONe', 'nOnE', 'nOne', 'noNE', 'noNe', 'nonE', 'none', 'NULL', 'NULl', 'NUlL', 'NUll', 'NuLL', 'NuLl', 'NulL', 'Null', 'nULL', 'nULl', 'nUlL', 'nUll', 'nuLL', 'nuLl', 'nulL', 'null', 'NA', 'Na', 'nA', 'na', 'N.A', 'N.a', 'N.A', 'N.a', 'n.A', 'n.a', 'n.A', 'n.a', 'N.A.', 'N.A.', 'N.a.', 'N.a.', 'N.A.', 'N.A.', 'N.a.', 'N.a.', 'n.A.', 'n.A.', 'n.a.', 'n.a.', 'n.A.', 'n.A.', 'n.a.', 'n.a.', 'NAN', 'NAn', 'NaN', 'Nan', 'nAN', 'nAn', 'naN', 'nan', 'NIL', 'NIl', 'NiL', 'Nil', 'nIL', 'nIl', 'niL', 'nil']
    """
    variations = (
        [None]
        + get_string_case_combination("none")
        + get_string_case_combination("null")
        + get_string_case_combination("na")
        + get_string_case_combination("n.a")
        + get_string_case_combination("n.a.")
        + get_string_case_combination("nan")
        + get_string_case_combination("nil")
    )
    return variations


def get_country_name_variation() -> dict:
    """Returns a dictionary mapping country names to their codes.

    Returns:
        dict: Dictionary with country names as keys and country codes as values.
    """
    variations = {
        "AFRICA": "BW",
        "BOSNIA": "BA",
        "CZECH REPUBLIC": "CZ",
        "MACEDONIA": "MK",
        "REPUBLIC OF CHINA": "CN",
        "REPUBLIC OF KOREA": "KR",
        "RUSSIAN FEDERATION": "RU",
        "SLAVONIC": "SK",
        "SLOVAK REPUBLIC": "SK",
        "TURKEY": "TR",
        "TURKIYE": "TR",
        "UNITED STATES": "US",
    }
    return variations


def get_country_code_variation() -> dict:
    """Returns a dictionary mapping country codes to their name variations.

    Returns:
        dict: Dictionary with country codes as keys and lists of country name variations as values.
    """
    variations = {
        "BA": ["BOSNIA"],
        "BW": ["AFRICA"],
        "CN": ["REPUBLIC OF CHINA"],
        "CZ": ["CZECH REPUBLIC"],
        "KR": ["REPUBLIC OF KOREA"],
        "MK": ["MACEDONIA"],
        "RU": ["RUSSIAN FEDERATION"],
        "SK": ["SLAVONIC", "SLOVAK REPUBLIC"],
        "TR": ["TURKEY", "TURKIYE"],
        "US": ["UNITED STATES"],
    }
    return variations


def list_tuple_without_none(list_tuple: list | tuple) -> list | tuple:
    """Removes None variations from a list or tuple.

    Args:
        list_tuple (list | tuple): Input list or tuple to clean.

    Returns:
        list | tuple: List or tuple with None variations removed.

    Raises:
        TypeError: If input is not a list or tuple.

    Examples:
        >>> list_tuple_without_none(["a", "none"])
        ['a']
        >>> list_tuple_without_none(("a", "none"))
        ('a',)
    """
    none_variations = get_none_variation()
    if isinstance(list_tuple, list):
        lt = [item for item in list_tuple if item and item not in none_variations]
    elif isinstance(list_tuple, tuple):
        lt = tuple(item for item in list_tuple if item and item not in none_variations)
    else:
        raise TypeError("Wrong datatype(s)")
    return lt


def string_boolean_to_int(boolean_str_rep: str) -> int:
    """Converts string boolean representations to integers.

    Deprecated: Will be removed in Python 3.12+

    Args:
        boolean_str_rep (str): String representation of boolean value.

    Returns:
        int: 1 for true values, 0 for false values.

    Examples:
        >>> string_boolean_to_int("true")
        1
        >>> string_boolean_to_int("True")
        1
    """
    from distutils.util import strtobool

    int_ = strtobool(string_str_to_str(boolean_str_rep))
    return int_


def string_dlt_to_dlt(dlt_str_rep: str) -> dict | list | tuple:
    """Converts string representation of data structures to actual Python objects.

    Args:
        dlt_str_rep (str): String representation of dictionary/list/tuple.

    Returns:
        dict | list | tuple: Converted Python data structure.

    Examples:
        >>> string_dlt_to_dlt("[1, 2, 3]")
        [1, 2, 3]
        >>> string_dlt_to_dlt("{'a': 1, 'b': 2}")
        {'a': 1, 'b': 2}
        >>> string_dlt_to_dlt("('1', '2', '3')")
        ('1', '2', '3')
    """
    from ast import literal_eval

    dlt = literal_eval(dlt_str_rep)
    return dlt


def string_str_to_str(string_str_rep: str) -> str:
    """Converts string representation to a clean string by removing quotes.

    Args:
        string_str_rep (str): String representation to clean.

    Returns:
        str: Cleaned string with outer quotes removed.

    Examples:
        >>> string_str_to_str("'test'")
        'test'
        >>> string_str_to_str('"test"')
        'test'
    """
    str_ = string_str_rep.strip("'\"")
    return str_


def delete_list_indices(list_: list, indices: list) -> None:
    """Deletes multiple indices from a list in-place.

    Args:
        list_ (list): Original list to modify.
        indices (list): List of indices to delete.

    Examples:
        >>> values = [0, 1, 2, 3, 4]
        >>> delete_list_indices(values, [1, 3])
        >>> values
        [0, 2, 4]
    """
    for index in sorted(indices, reverse=True):
        del list_[index]


# FILESYSTEM
def get_path_files(path: str, keywords: list) -> list:
    """Returns sorted list of files from given path that contain specified keywords.

    Args:
        path (str): Directory path to search.
        keywords (list): List of keywords to match in filenames.

    Returns:
        list: Sorted list of matching filenames.

    Examples:
        >>> get_path_files("test_base_folder", ["py"])
        ['test1.py', 'test2.py', 'test3.py', 'test4.py', 'test5.py']
    """

    list_ = []
    for file in sorted(os.listdir(path)):
        if any(word in file for word in keywords):
            list_.append(file)
    return list_


def remove_path_file(path: str, keyword: str, n: int = 2) -> None:
    """Removes all but the n newest files matching the keyword from the specified path.

    Args:
        path (str): Directory path.
        keyword (str): Keyword to match in filenames.
        n (int, optional): Number of newest files to keep. Defaults to 2.

    Examples:
        >>> remove_path_file("test_base_folder", ".py")
    """

    to_delete = get_path_files(path=path, keywords=[keyword])[:-n]
    for file in to_delete:
        os.remove(f"{path}/{file}")
        logger.info(f"{path}/{file} deleted ...")


def list_to_file(filepath: str, list_: list, newline: bool = True) -> None:
    """Writes list contents to a file.

    Args:
        filepath (str): Path to output file.
        list_ (list): List of values to write.
        newline (bool, optional): Whether to add newline after each item. Defaults to True.

    Examples:
        >>> list_to_file("test.txt", [1, 2, 3])
    """
    f = open(filepath, "w")
    for line in list_:
        f.write(str(line))
        if newline:
            f.write("\n")
    f.close()


# URLLIB
def create_encode_url(url: str, query_params: dict = {}) -> str:
    """Creates an encoded URL with query parameters.

    Args:
        url (str): Base URL.
        query_params (dict, optional): Dictionary of URL query parameters. Defaults to {}.

    Returns:
        str: Encoded URL with query parameters.
    """
    from urllib.parse import urlencode

    return f"{url}{urlencode(query_params)}"


# SYSTEM
def parse_ps_aux(ps_aux_commands: str) -> List[list]:
    """Parses Linux ps aux command output into a list of records.

    Args:
        ps_aux_commands (str): Linux ps aux command string.

    Returns:
        List[list]: List of lists, where each inner list represents a process record.

    Examples:
        >>> # parse_ps_aux("ps aux | egrep -i '%cpu|anaconda3' | head")
    """
    output = subprocess.run(
        ps_aux_commands,
        capture_output=True,
        text=True,
        shell=True,
        executable="/bin/bash",
    )
    lines = output.stdout.split("\n")
    n_columns = len(lines[0].split()) - 1
    rows = [line.split(None, n_columns) for line in lines if line]
    return rows


if __name__ == "__main__":
    import doctest

    subprocess.run(
        "mkdir -p test_base_folder",
        capture_output=True,
        shell=True,
        text=True,
        executable="/bin/bash",
    )
    subprocess.run(
        "touch test_base_folder/test{1..5}.py",
        capture_output=True,
        shell=True,
        text=True,
        executable="/bin/bash",
    )
    doctest.testmod()
    subprocess.run(
        "rm -rf test_base_folder",
        capture_output=True,
        shell=True,
        text=True,
        executable="/bin/bash",
    )
    subprocess.run(
        "rm test.txt",
        capture_output=True,
        shell=True,
        text=True,
        executable="/bin/bash",
    )
