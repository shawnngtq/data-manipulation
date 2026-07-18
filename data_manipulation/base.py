import itertools
import os
import re
import subprocess
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse

try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


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


def get_string_case_combination(str_: str) -> list[str]:
    """Generates all possible case combinations of a string.

    Args:
        str_ (str): Input string to generate combinations for.

    Returns:
        list[str]: List of all possible case combinations.

    Examples:
        >>> get_string_case_combination("abc")
        ['ABC', 'ABc', 'AbC', 'Abc', 'aBC', 'aBc', 'abC', 'abc']
    """
    if not str_:
        raise ValueError("Input string cannot be empty or None")

    return list(
        map(
            "".join,
            itertools.product(*zip(str_.upper(), str_.lower(), strict=True)),
        )
    )


def get_none_variation() -> frozenset:
    """Returns a frozenset of lowercase None/null string representations.

    Use with .casefold() for case-insensitive membership tests:
        item.casefold() in get_none_variation()

    Returns:
        frozenset: Frozenset of lowercase null string representations.

    Examples:
        >>> "none" in get_none_variation()
        True
        >>> "NONE".casefold() in get_none_variation()
        True
        >>> "valid".casefold() in get_none_variation()
        False
    """
    return frozenset({"none", "null", "na", "n.a", "n.a.", "nan", "nil"})


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


def list_tuple_without_none(
    list_tuple: list | tuple,
) -> list | tuple:
    """Removes None variations from a list or tuple.

    Args:
        list_tuple: Input list or tuple to clean.

    Returns:
        list | tuple: Cleaned list or tuple.

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

    def _keep(item: Any) -> bool:
        if not item:
            return False
        if isinstance(item, str):
            return item.casefold() not in none_variations
        return True

    if isinstance(list_tuple, list):
        return [item for item in list_tuple if _keep(item)]
    return tuple(item for item in list_tuple if _keep(item))


def delete_list_indices(
    list_: list,
    indices: list[int],
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
    path: str | Path,
    keywords: list[str],
) -> list[str]:
    """Returns sorted list of files from given path that contain specified keywords.

    Args:
        path (str): Directory path to search.
        keywords (list): List of keywords to match in filenames.

    Returns:
        list[str]: Sorted list of matching filenames.

    Examples:
        >>> get_path_files("test_base_folder", ["py"])  # doctest: +SKIP
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
    path: str | Path,
    keyword: str,
    n: int = 2,
) -> None:
    """Removes all but the n newest files matching the keyword.

    Args:
        path (str): Directory path.
        keyword (str): Keyword to match in filenames.
        n (int, optional): Number of newest files to keep. Defaults to 2.

    Examples:
        >>> remove_path_file("test_base_folder", ".py")  # doctest: +SKIP
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
    filepath: str | Path,
    list_: list,
    newline: bool = True,
) -> None:
    """Writes list contents to a file.

    Args:
        filepath (str): Path to output file.
        list_ (list): List of values to write.
        newline (bool, optional): Whether to add newline after each item. Defaults to True.

    Examples:
        >>> list_to_file("test.txt", [1, 2, 3])  # doctest: +SKIP
    """
    filepath = Path(filepath)
    try:
        with filepath.open("w") as f:
            for line in list_:
                f.write(str(line))
                if newline:
                    f.write("\n")
        logger.info(f"Successfully wrote to file: {filepath}")
    except OSError as e:
        logger.error(f"Failed to write to file {filepath}: {e}")
        raise


# URLLIB
def create_encode_url(
    url: str,
    query_params: dict[str, Any] | None = None,
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
def parse_ps_aux(ps_aux_commands: str) -> list[list[str]]:
    """Parses Linux ps aux command output into a list of records.

    Args:
        ps_aux_commands (str): Linux ps aux command string.

    Returns:
        list[list[str]]: List of process records.

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
        if not lines or not lines[0]:
            return []

        if "PID" not in lines[0] and "USER" not in lines[0]:
            logger.warning(
                "ps output header looks unexpected; "
                "column split may be incorrect"
            )
        n_columns = len(lines[0].split()) - 1
        return [line.split(None, n_columns) for line in lines if line]
    except subprocess.SubprocessError as e:
        logger.error(f"Failed to execute command: {e}")
        raise
