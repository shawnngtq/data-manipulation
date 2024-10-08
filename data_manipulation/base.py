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
    """
    Return given string that is strip, uppercase without multiple whitespaces. Optionally, remove parenthesis and brackets. Note that tab, newline, whitespace will be removed

    Parameters
    ----------
    string : str
        String to clean
    remove_parenthesis : bool, optional
        To remove parenthesis, by default False
    remove_brackets : bool, optional
        To remove brackets, by default False

    Examples
    --------
    >>> clean_string(" sHawn  tesT ")
    'SHAWN TEST'
    >>> clean_string("shawn ( te  st )")
    'SHAWN ( TE ST )'
    >>> clean_string("shawn ( te  st )", remove_parenthesis=True)
    'SHAWN'
    >>> clean_string("shawn [ te  st ]", remove_brackets=True)
    'SHAWN'

    Returns
    -------
    str
        Uppercase cleaned string
    """

    if remove_parenthesis:
        string = re.sub(r"\(.*\)", "", string)
    if remove_brackets:
        string = re.sub(r"\[.*\]", "", string)

    string = string.strip().upper()
    string = " ".join(string.split())
    return string


def get_string_case_combination(str_: str) -> list:
    """
    Get list of case combination of a string. Reference: https://stackoverflow.com/questions/11144389/find-all-upper-lower-and-mixed-case-combinations-of-a-string

    Parameters
    ----------
    str_ : str
        Input string

    Examples
    --------
    >>> get_string_case_combination("abc")
    ['ABC', 'ABc', 'AbC', 'Abc', 'aBC', 'aBc', 'abC', 'abc']

    Returns
    -------
    list
        Case combinations
    """

    return list(map("".join, itertools.product(*zip(str_.upper(), str_.lower()))))


def get_none_variation() -> list:
    """
    Get List of none variation

    Examples
    --------
    >>> get_none_variation()
    [None, 'NONE', 'NONe', 'NOnE', 'NOne', 'NoNE', 'NoNe', 'NonE', 'None', 'nONE', 'nONe', 'nOnE', 'nOne', 'noNE', 'noNe', 'nonE', 'none', 'NULL', 'NULl', 'NUlL', 'NUll', 'NuLL', 'NuLl', 'NulL', 'Null', 'nULL', 'nULl', 'nUlL', 'nUll', 'nuLL', 'nuLl', 'nulL', 'null', 'NA', 'Na', 'nA', 'na', 'N.A', 'N.a', 'N.A', 'N.a', 'n.A', 'n.a', 'n.A', 'n.a', 'N.A.', 'N.A.', 'N.a.', 'N.a.', 'N.A.', 'N.A.', 'N.a.', 'N.a.', 'n.A.', 'n.A.', 'n.a.', 'n.a.', 'n.A.', 'n.A.', 'n.a.', 'n.a.', 'NAN', 'NAn', 'NaN', 'Nan', 'nAN', 'nAn', 'naN', 'nan', 'NIL', 'NIl', 'NiL', 'Nil', 'nIL', 'nIl', 'niL', 'nil']

    Returns
    -------
    list
        None variation
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
    """
    Return the given list / tuple without None variation

    Parameters
    ----------
    list_tuple : list | tuple
        Standard list or tuple

    Examples
    --------
    >>> list_tuple_without_none(["a", "none"])
    ['a']
    >>> list_tuple_without_none(("a", "none"))
    ('a',)
    >>> list_tuple_without_none(get_none_variation())
    []
    >>> list_tuple_without_none(["a", "none", ""])
    ['a']
    >>> list_tuple_without_none(("a", "none", ""))
    ('a',)

    Returns
    -------
    list | tuple
        List without none variation

    Raises
    ------
    TypeError
        Only list or tuple datatypes
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
    """
    DEPRECATED from python 3.12 onwards

    Return integer from given string boolean. 1 instead of "true"/"True"/"1". Reference from https://docs.python.org/3/distutils/apiref.html#distutils.util.strtobool

    Parameters
    ----------
    boolean_str_rep : str
        String representation of boolean

    Notes
    -----
    Do not use eval() as it's unsafe

    Examples
    --------
    >>> string_boolean_to_int("true")
    1
    >>> string_boolean_to_int("True")
    1
    >>> string_boolean_to_int("1")
    1

    Returns
    -------
    int
        _description_

    Reference
    ---------
    - https://docs.python.org/3.10/library/distutils.html
    """
    from distutils.util import strtobool

    int_ = strtobool(string_str_to_str(boolean_str_rep))
    return int_


def string_dlt_to_dlt(dlt_str_rep: str) -> dict | list | tuple:
    """
    Return dictionary/list/tuple from given string representation of dictionary/list/tuple

    Parameters
    ----------
    dlt_str_rep : str
        String representation of dictionary/list/tuple

    Examples
    --------
    >>> string_dlt_to_dlt("[1, 2, 3]")
    [1, 2, 3]
    >>> string_dlt_to_dlt("[]")
    []
    >>> string_dlt_to_dlt("['1', '2', '3']")
    ['1', '2', '3']
    >>> string_dlt_to_dlt("{'a': 1, 'b': 2}")
    {'a': 1, 'b': 2}
    >>> string_dlt_to_dlt("{'a': '1', 'b': '2'}")
    {'a': '1', 'b': '2'}
    >>> string_dlt_to_dlt("('1', '2', '3')")
    ('1', '2', '3')

    Returns
    -------
    dict | list | tuple
        _description_
    """
    from ast import literal_eval

    dlt = literal_eval(dlt_str_rep)
    return dlt


def string_str_to_str(string_str_rep: str) -> str:
    """
    Return string from given string representation of string

    Parameters
    ----------
    string_str_rep : str
        String representation of string

    Examples
    --------
    >>> string_str_to_str("")
    ''
    >>> string_str_to_str('test')
    'test'
    >>> string_str_to_str("'test'")
    'test'
    >>> string_str_to_str('"test"')
    'test'

    Returns
    -------
    str
        _description_
    """
    str_ = string_str_rep.strip("'\"")
    return str_


def delete_list_indices(list_: list, indices: list) -> None:
    """
    Delete multiple indices in a list

    Parameters
    ----------
    list_ : list
        Original list
    indices : list
        List of indices to delete

    Examples
    --------
    >>> values = [0, 1, 2, 3, 4]
    >>> delete_list_indices(values, [1, 3])
    >>> values
    [0, 2, 4]
    """
    for index in sorted(indices, reverse=True):
        del list_[index]


# FILESYSTEM
def get_path_files(path: str, keywords: list) -> list:
    """
    Return sorted list of files from given path and keywords

    Parameters
    ----------
    path : str
        File path
    keywords : list
        List of keywords

    Examples
    --------
    >>> get_path_files("test_base_folder", ["py"])
    ['test1.py', 'test2.py', 'test3.py', 'test4.py', 'test5.py']

    Returns
    -------
    list
        Sorted list
    """

    list_ = []
    for file in sorted(os.listdir(path)):
        if any(word in file for word in keywords):
            list_.append(file)
    return list_


def remove_path_file(path: str, keyword: str, n: int = 2) -> None:
    """
    Remove n oldest files from given path and keyword

    Parameters
    ----------
    path : str
        Directory path
    keyword : str
        Keyword
    n : int, optional
        Keep latest n files, by default 2

    Examples
    --------
    >>> get_path_files("test_base_folder", ["py"])
    ['test1.py', 'test2.py', 'test3.py', 'test4.py', 'test5.py']
    >>> remove_path_file("test_base_folder", ".py")
    test_base_folder/test1.py deleted ...
    test_base_folder/test2.py deleted ...
    test_base_folder/test3.py deleted ...
    >>> get_path_files("test_base_folder", ["py"])
    ['test4.py', 'test5.py']
    """

    to_delete = get_path_files(path=path, keywords=[keyword])[:-n]
    for file in to_delete:
        os.remove(f"{path}/{file}")
        logger.info(f"{path}/{file} deleted ...")


def list_to_file(filepath: str, list_: list, newline: bool = True) -> None:
    """
    Export list to file

    Parameters
    ----------
    filepath : str
        Filepath
    list_ : list
        List of values
    newline : bool, optional
        Add newline to end of file, by default True

    Examples
    --------
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
    """
    Create encoded url

    Parameters
    ----------
    url : str
        base url
    query_params : dict, optional
        dictionary of url query parameters, by default {}

    Returns
    -------
    str
        encoded url
    """
    from urllib.parse import urlencode

    return f"{url}{urlencode(query_params)}"


# SYSTEM
def parse_ps_aux(ps_aux_commands: str) -> List[list]:
    """
    Parse linux ps aux related-command to list of lists

    Parameters
    ----------
    ps_aux_commands : str
        Linux ps aux

    Examples
    --------
    >>> # parse_ps_aux("ps aux | egrep -i '%cpu|anaconda3' | head")

    Returns
    -------
    List[list]
        Each list represent a record
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
