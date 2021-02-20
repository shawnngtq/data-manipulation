def string_str_to_str(string_str_rep):
    """
    Convert string representation of string to string

    Parameters
    ----------
    string_str_rep: str
        "'str'"

    Examples
    --------
    >>> string_str_to_str(1)
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
        String instead of string representation
    """
    output = None
    if isinstance(string_str_rep, (str)):
        output = string_str_rep.strip("'\"")
    return output


def string_boolean_to_int(boolean_str_rep):
    """
    Convert string boolean to int
    https://docs.python.org/3/distutils/apiref.html#distutils.util.strtobool

    Parameters
    ----------
    boolean_str_rep: str
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
    >>> string_boolean_to_int(1)

    Returns
    -------
    int
        0 or 1 instead of "0" or "1"
    """
    output = None
    if isinstance(boolean_str_rep, (str)):
        from distutils.util import strtobool
        output = strtobool(string_str_to_str(boolean_str_rep))
    return output


def string_dlt_to_dlt(dlt_str_rep):
    """
    Convert string representation of dictionary/list/tuple to dictionary/list/tuple

    Parameters
    ----------
    dlt_str_rep: str
        '[]'

    Examples
    --------
    >>> string_dlt_to_dlt("")
    Traceback (most recent call last):
      ...
    SyntaxError: unexpected EOF while parsing
    >>> string_dlt_to_dlt(0)
    >>> string_dlt_to_dlt([])
    >>> string_dlt_to_dlt({})
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
    dict / list / tuple
        Dictionary / List / Tuple instead of string representation
    """
    output = None
    if isinstance(dlt_str_rep, (str)):
        from ast import literal_eval
        output = literal_eval(dlt_str_rep)
    return output


def get_path_file(path, keywords):
    """
    Given path and keywords, return latest file

    Parameters
    ----------
    path: str
        Directory path
    keywords: list
        List of keywords

    Examples
    --------
    >>> get_path_file("data_manipulation/base", ["py"])
    List of files: ['__init__.py', 'base_.py']
    'base_.py'

    Returns
    -------
    str
        Latest file name in directory
    """
    import os

    output = []
    for file in sorted(os.listdir(path)):
        if all(word in file for word in keywords):
            output.append(file)
    print(f"List of files: {sorted(output)}")
    return max(output)


def get_none_variation():
    """
    Get all none variation

    Examples
    --------
    >>> get_none_variation()
    [None, 'none', 'None', 'NONE', 'null', 'Null', 'NULL', 'na', 'Na', 'nA', 'NA', 'N.A', 'N.A.', 'nil', 'Nil', 'NIL']

    Returns
    -------
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
    list_tuple: list / tuple
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
    List / Tuple without None variation
        [anyvalue, anytype, anylength]
    """
    none_variations = get_none_variation()
    output = None
    if isinstance(list_tuple, list):
        output = [item for item in list_tuple if item and item not in none_variations]
    elif isinstance(list_tuple, tuple):
        output = tuple(item for item in list_tuple if item and item not in none_variations)
    return output


def clean_string(string, remove_parenthesis=False, remove_brackets=False):
    """
    Clean string by striping, uppercase and remove multiple whitespaces

    Parameters
    ----------
    string: str
        String to clean
    remove_parenthesis: bool
        To remove parenthesis
    remove_brackets: bool
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
    Return cleaned string. Note that \t\n\s will be removed
    """
    import re

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
