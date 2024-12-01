import itertools
import os
import re
import subprocess
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlencode, urlparse, urlunparse

from loguru import logger


def deprecated(func):
    """Decorator to mark functions as deprecated."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.warning(
            f"Function {func.__name__} is deprecated and will be removed in future versions"
        )
        return func(*args, **kwargs)

    return wrapper


# DATA STRUCTURE
def clean_string(
    string: str,
    remove_parenthesis: bool = False,
    remove_brackets: bool = False,
) -> str:
    """Cleans and standardizes input string.

    Args:
        string (str): String to clean.
        remove_parenthesis (bool): Whether to remove content within parentheses.
        remove_brackets (bool): Whether to remove content within square brackets.

    Returns:
        str: Uppercase cleaned string with standardized spacing.

    Raises:
        ValueError: If input string is None or empty.

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
    if not string or not isinstance(string, str):
        raise ValueError("Input must be a non-empty string")

    if remove_parenthesis:
        string = re.sub(r"\(.*\)", "", string)
    if remove_brackets:
        string = re.sub(r"\[.*\]", "", string)

    return " ".join(string.strip().upper().split())


def get_string_case_combination(str_: str) -> List[str]:
    """Generates all possible case combinations of a string.

    Args:
        str_ (str): Input string to generate combinations for.

    Returns:
        List[str]: List of all possible case combinations.

    Examples:
        >>> get_string_case_combination("abc")
        ['ABC', 'ABc', 'AbC', 'Abc', 'aBC', 'aBc', 'abC', 'abc']
    """
    if not str_:
        raise ValueError("Input string cannot be empty or None")

    return list(map("".join, itertools.product(*zip(str_.upper(), str_.lower()))))


def get_none_variation() -> List[Union[None, str]]:
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


def list_tuple_without_none(list_tuple: Union[List, Tuple]) -> Union[List, Tuple]:
    """Removes None variations from a list or tuple.

    Args:
        list_tuple: Input list or tuple to clean.

    Returns:
        Union[List, Tuple]: Cleaned list or tuple.

    Raises:
        TypeError: If input is not a list or tuple.

    Examples:
        >>> list_tuple_without_none(["a", "none"])
        ['a']
        >>> list_tuple_without_none(("a", "none"))
        ('a',)
    """
    if not isinstance(list_tuple, (list, tuple)):
        raise TypeError("Input must be a list or tuple")

    none_variations = get_none_variation()

    if isinstance(list_tuple, list):
        return [item for item in list_tuple if item and item not in none_variations]
    return tuple(item for item in list_tuple if item and item not in none_variations)


@deprecated
def string_boolean_to_int(boolean_str_rep: str) -> int:
    """Converts string boolean representations to integers.

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
    try:
        from distutils.util import strtobool

        return strtobool(string_str_to_str(boolean_str_rep))
    except ValueError as e:
        raise ValueError(
            f"Invalid boolean string representation: {boolean_str_rep}"
        ) from e


def string_dlt_to_dlt(dlt_str_rep: str) -> Union[Dict, List, Tuple]:
    """Converts string representation of data structures to actual Python objects.

    Args:
        dlt_str_rep (str): String representation of dictionary/list/tuple.

    Returns:
        Union[Dict, List, Tuple]: Converted Python data structure.

    Examples:
        >>> string_dlt_to_dlt("[1, 2, 3]")
        [1, 2, 3]
        >>> string_dlt_to_dlt("{'a': 1, 'b': 2}")
        {'a': 1, 'b': 2}
        >>> string_dlt_to_dlt("('1', '2', '3')")
        ('1', '2', '3')
    """
    try:
        from ast import literal_eval

        return literal_eval(dlt_str_rep)
    except (ValueError, SyntaxError) as e:
        raise ValueError(f"Invalid data structure string: {dlt_str_rep}") from e


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
    if not string_str_rep:
        raise ValueError("Input string cannot be empty or None")
    return string_str_rep.strip("'\"")


def delete_list_indices(
    list_: list,
    indices: List[int],
) -> None:
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
def get_path_files(
    path: Union[str, Path],
    keywords: List[str],
) -> List[str]:
    """Returns sorted list of files from given path that contain specified keywords.

    Args:
        path (str): Directory path to search.
        keywords (list): List of keywords to match in filenames.

    Returns:
        List[str]: Sorted list of matching filenames.

    Examples:
        >>> get_path_files("test_base_folder", ["py"])
        ['test1.py', 'test2.py', 'test3.py', 'test4.py', 'test5.py']
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    return sorted(
        f.name
        for f in path.iterdir()
        if f.is_file() and any(word in f.name for word in keywords)
    )


def remove_path_file(
    path: Union[str, Path],
    keyword: str,
    n: int = 2,
) -> None:
    """Removes all but the n newest files matching the keyword.

    Args:
        path (str): Directory path.
        keyword (str): Keyword to match in filenames.
        n (int, optional): Number of newest files to keep. Defaults to 2.

    Examples:
        >>> remove_path_file("test_base_folder", ".py")
    """
    if n < 0:
        raise ValueError("n must be non-negative")

    path = Path(path)
    to_delete = get_path_files(path=path, keywords=[keyword])[:-n]

    for file in to_delete:
        try:
            file_path = path / file
            file_path.unlink()
            logger.info(f"Deleted file: {file_path}")
        except OSError as e:
            logger.error(f"Failed to delete {file_path}: {e}")


def list_to_file(
    filepath: Union[str, Path],
    list_: List,
    newline: bool = True,
) -> None:
    """Writes list contents to a file.

    Args:
        filepath (str): Path to output file.
        list_ (list): List of values to write.
        newline (bool, optional): Whether to add newline after each item. Defaults to True.

    Examples:
        >>> list_to_file("test.txt", [1, 2, 3])
    """
    filepath = Path(filepath)
    try:
        with filepath.open("w") as f:
            for line in list_:
                f.write(str(line))
                if newline:
                    f.write("\n")
        logger.info(f"Successfully wrote to file: {filepath}")
    except IOError as e:
        logger.error(f"Failed to write to file {filepath}: {e}")
        raise


# URLLIB
def create_encode_url(
    url: str,
    query_params: Optional[Dict[str, Any]] = None,
) -> str:
    """Creates an encoded URL with query parameters.

    Args:
        url (str): Base URL.
        query_params (dict, optional): Dictionary of URL query parameters. Defaults to {}.

    Returns:
        str: Encoded URL with query parameters.

    Raises:
        ValueError: If URL is invalid.
    """
    query_params = query_params or {}

    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid URL format")

        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urlencode(query_params),
                parsed.fragment,
            )
        )
    except Exception as e:
        logger.error(f"Failed to encode URL {url}: {e}")
        raise


# SYSTEM
def parse_ps_aux(ps_aux_commands: str) -> List[List[str]]:
    """Parses Linux ps aux command output into a list of records.

    Args:
        ps_aux_commands (str): Linux ps aux command string.

    Returns:
        List[List[str]]: List of process records.

    Examples:
        >>> # parse_ps_aux("ps aux | egrep -i '%cpu|anaconda3' | head")
    """
    try:
        output = subprocess.run(
            ps_aux_commands,
            capture_output=True,
            text=True,
            shell=True,
            executable="/bin/bash",
            check=True,
        )
        lines = output.stdout.strip().split("\n")
        if not lines:
            return []

        n_columns = len(lines[0].split()) - 1
        return [line.split(None, n_columns) for line in lines if line]
    except subprocess.SubprocessError as e:
        logger.error(f"Failed to execute command: {e}")
        raise


if __name__ == "__main__":
    import doctest

    # Setup test environment
    test_folder = Path("test_base_folder")
    try:
        test_folder.mkdir(exist_ok=True)
        for i in range(1, 6):
            (test_folder / f"test{i}.py").touch()

        doctest.testmod()
    finally:
        # Cleanup
        if test_folder.exists():
            for file in test_folder.iterdir():
                file.unlink()
            test_folder.rmdir()

        test_file = Path("test.txt")
        if test_file.exists():
            test_file.unlink()
