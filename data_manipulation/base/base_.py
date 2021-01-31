def clean_string_representation(string_str_rep):
    """
    Convert string representation of string to string

    Parameters
    ----------
    string_str_rep: str
        "'str'"

    Examples
    --------
    >>> input = 1
    >>> clean_string_representation(input)
    >>> input = ""
    >>> clean_string_representation(input)
    ''
    >>> input = 'test'
    >>> clean_string_representation(input)
    'test'
    >>> input = "'test'"
    >>> clean_string_representation(input)
    'test'
    >>> input = '"test"'
    >>> clean_string_representation(input)
    'test'

    Returns
    -------
    str
        String instead of string representation
    """
    string = None
    if isinstance(string_str_rep, (str)):
        string = string_str_rep.strip("'\"")
    return string


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
    >>> input = "true"
    >>> string_boolean_to_int(input)
    1
    >>> input = "True"
    >>> string_boolean_to_int(input)
    1
    >>> input = "1"
    >>> string_boolean_to_int(input)
    1
    >>> input = 1
    >>> string_boolean_to_int(input)

    Returns
    -------
    int
        0 or 1 instead of "0" or "1"
    """
    boolean = None
    if isinstance(boolean_str_rep, (str)):
        from distutils.util import strtobool
        boolean = strtobool(clean_string_representation(boolean_str_rep))
    return boolean


def string_to_dict_list(dictionary_str_rep):
    """
    Convert string representation of dictionary/list to dictionary/list

    Parameters
    ----------
    dictionary_str_rep: str
        '[]'

    Examples
    --------
    >>> input = ""
    >>> string_to_dict_list(input)
    Traceback (most recent call last):
      ...
    SyntaxError: unexpected EOF while parsing
    >>> input = 0
    >>> string_to_dict_list(input)
    >>> input = []
    >>> string_to_dict_list(input)
    >>> input = {}
    >>> string_to_dict_list(input)
    >>> input = "[1, 2, 3]"
    >>> string_to_dict_list(input)
    [1, 2, 3]
    >>> input = "[]"
    >>> string_to_dict_list(input)
    []
    >>> input = "['1', '2', '3']"
    >>> string_to_dict_list(input)
    ['1', '2', '3']
    >>> input = "{'a': 1, 'b': 2}"
    >>> string_to_dict_list(input)
    {'a': 1, 'b': 2}
    >>> input = "{'a': '1', 'b': '2'}"
    >>> string_to_dict_list(input)
    {'a': '1', 'b': '2'}

    Returns
    -------
    dict / list
        Dictionary / List instead of string representation
    """
    dict_list = None
    if isinstance(dictionary_str_rep, (str)):
        from ast import literal_eval
        dict_list = literal_eval(dictionary_str_rep)
    return dict_list


def get_latest_file(path, keywords):
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
    >>> get_latest_file("data_manipulation/base", ["py"])
    List of files: ['__init__.py', '__pycache__', 'base_.py']
    'base_.py'

    Returns
    -------
    str
        Latest file name in directory
    """
    import os

    files = []
    for file in sorted(os.listdir(path)):
        if all(word in file for word in keywords):
            files.append(file)
    print(f"List of files: {sorted(files)}")
    return max(files)


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


def clean_list(list_):
    """
    Clean a given list

    Parameters
    ----------
    list_: list
        [anyvalue, anytype, anylength]

    Examples
    --------
    >>> clean_list(["a", "none"])
    ['a']
    >>> clean_list(get_none_variation())
    []
    >>> clean_list(["a", "none", ""])
    ['a']

    Returns
    -------
    List without None variation
        [anyvalue, anytype, anylength]
    """
    none_variations = get_none_variation()
    cleaned_list = [item for item in list_ if item and item not in none_variations]
    return cleaned_list


def clean_string(string, remove_parenthesis=False, remove_brackets=False):
    """
    Clean string by striping, uppercase and remove multiple whitespaces

    Parameters
    ----------
    string: str
        String to clean

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
