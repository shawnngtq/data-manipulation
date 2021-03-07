def string_str_to_str(string_str_rep):
    """
    Convert string representation of string to string

    Parameters
    ----------
    string_str_rep : str
        "'str'"

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
    str_ : str
        String instead of string representation
    """
    if not isinstance(string_str_rep, str):
        raise TypeError("Argument must be str ...")
    str_ = string_str_rep.strip("'\"")
    return str_


def string_boolean_to_int(boolean_str_rep):
    """
    Convert string boolean to int
    https://docs.python.org/3/distutils/apiref.html#distutils.util.strtobool

    Parameters
    ----------
    boolean_str_rep : str
        "True"

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
    int_ : int
        0 or 1 instead of "0" or "1"
    """
    from distutils.util import strtobool

    if not isinstance(boolean_str_rep, str):
        raise TypeError("Argument must be str ...")
    int_ = strtobool(string_str_to_str(boolean_str_rep))
    return int_


def string_dlt_to_dlt(dlt_str_rep):
    """
    Convert string representation of dictionary/list/tuple to dictionary/list/tuple

    Parameters
    ----------
    dlt_str_rep : str
        '[]'

    Examples
    --------
    >>> string_dlt_to_dlt("")
    Traceback (most recent call last):
      ...
    SyntaxError: unexpected EOF while parsing
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
    dlt : dict or list or tuple
        Dictionary or List or Tuple instead of string representation
    """
    from ast import literal_eval

    if not isinstance(dlt_str_rep, str):
        raise TypeError("Argument must be str ...")
    dlt = literal_eval(dlt_str_rep)
    return dlt


def get_path_files(path, keywords):
    """
    Given path and keywords, return latest file

    Parameters
    ----------
    path : str
        Directory path
    keywords : list
        List of keywords

    Examples
    --------
    >>> get_path_files("data_manipulation/base", [".py"])
    ['__init__.py', 'base_.py']

    Returns
    -------
    list_ : list
        List of files in directory
    """
    import os

    if not isinstance(path, str):
        raise TypeError("Argument 1 must be str ...")
    if not isinstance(keywords, list):
        raise TypeError("Argument 2 must be list ...")

    list_ = []
    for file in sorted(os.listdir(path)):
        if any(word in file for word in keywords):
            list_.append(file)
    return list_


def remove_path_file(path, keywords, n=2):
    """
    Keep latest n files of stated keywords

    Parameters
    ----------
    path: str
        Directory path
    keywords: list
        List of file keywords
    n: int, optional
        Keep latest n files

    Returns
    -------
    None
        Remove all files with specified keywords that are not latest n
    """
    import os

    if not isinstance(path, str):
        raise TypeError("Argument 1 must be str ...")
    if not isinstance(keywords, list):
        raise TypeError("Argument 2 must be list ...")
    if not isinstance(n, int):
        raise TypeError("Argument 3 must be int ...")

    to_delete = get_path_files(path=path, keywords=keywords)[:-len(keywords)*n]
    for file in to_delete:
        os.remove(f"{path}/{file}")
        print(f"{path}/{file} deleted ...")


def get_none_variation():
    """
    Get all none variation

    Examples
    --------
    >>> get_none_variation()
    [None, 'none', 'None', 'NONE', 'null', 'Null', 'NULL', 'na', 'Na', 'nA', 'NA', 'N.A', 'N.A.', 'nil', 'Nil', 'NIL']

    Returns
    -------
    variation : list
        List of None variation
    """
    variations = [
        None, "none", "None", "NONE",
        "null", "Null", "NULL",
        "na", "Na", "nA", "NA", "N.A", "N.A.",
        "nil", "Nil", "NIL",
    ]
    return variations


def truthy_list_tuple(list_tuple):
    """
    Clean a given list / tuple

    Parameters
    ----------
    list_tuple : list or tuple
        [anyvalue, anytype, anylength]

    Examples
    --------
    >>> truthy_list_tuple(["a", "none"])
    ['a']
    >>> truthy_list_tuple(("a", "none"))
    ('a',)
    >>> truthy_list_tuple(get_none_variation())
    []
    >>> truthy_list_tuple(["a", "none", ""])
    ['a']
    >>> truthy_list_tuple(("a", "none", ""))
    ('a',)

    Returns
    -------
    lt : List or Tuple
        [anyvalue, anytype, anylength] without None variation
    """
    none_variations = get_none_variation()
    if isinstance(list_tuple, list):
        lt = [item for item in list_tuple if item and item not in none_variations]
    elif isinstance(list_tuple, tuple):
        lt = tuple(item for item in list_tuple if item and item not in none_variations)
    else:
        raise TypeError("Argument must be list or tuple ...")
    return lt


def clean_string(string, remove_parenthesis=False, remove_brackets=False):
    """
    Clean string by striping, uppercase and remove multiple whitespaces

    Parameters
    ----------
    string : str
        String to clean
    remove_parenthesis : bool, optional
        To remove parenthesis
    remove_brackets : bool, optional
        To remove brackets

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
    string : str
        Return cleaned string. Note that \t\n\s will be removed
    """
    import re

    if not isinstance(string, str):
        raise TypeError("Argument 1 must be str ...")

    if remove_parenthesis:
        string = re.sub(r"\(.*\)", "", string)
    if remove_brackets:
        string = re.sub(r"\[.*\]", "", string)

    string = string.strip().upper()
    string = " ".join(string.split())
    return string


if __name__ == "__main__":
    import doctest

    doctest.testmod()
