def clean_string(string, remove_parenthesis=False, remove_brackets=False):
    """
    Return given string that is strip, uppercase without multiple whitespaces. Optionally, remove parenthesis and brackets. Note that "\t\n\s" will be removed

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


def get_none_variation():
    """
    Get List of none variation

    Examples
    --------
    >>> get_none_variation()
    [None, 'none', 'None', 'NONE', 'null', 'Null', 'NULL', 'na', 'Na', 'nA', 'NA', 'N.A', 'N.A.', 'nil', 'Nil', 'NIL']

    Returns
    -------
    variations : list
    """
    variations = [
        None, "none", "None", "NONE",
        "null", "Null", "NULL",
        "na", "Na", "nA", "NA", "N.A", "N.A.",
        "nil", "Nil", "NIL",
    ]
    return variations


def get_path_files(path, keywords):
    """
    Return sorted list of files from given path and keywords

    Parameters
    ----------
    path : str
        Path
    keywords : list
        List of keywords

    Examples
    --------
    >>> get_path_files("test_base_folder", ["py"])
    ['test1.py', 'test2.py', 'test3.py', 'test4.py', 'test5.py']

    Returns
    -------
    list_ : list
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


def list_tuple_without_none(list_tuple):
    """
    Return the given list / tuple without None variation

    Parameters
    ----------
    list_tuple : list or tuple
        [anyvalue, anytype, anylength]

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
    lt : List or Tuple
    """
    none_variations = get_none_variation()
    if isinstance(list_tuple, list):
        lt = [item for item in list_tuple if item and item not in none_variations]
    elif isinstance(list_tuple, tuple):
        lt = tuple(item for item in list_tuple if item and item not in none_variations)
    else:
        raise TypeError("Argument must be list or tuple ...")
    return lt


def remove_path_file(path, keyword, n=2):
    """
    Remove (n-2) oldest files from given path and keyword

    Parameters
    ----------
    path: str
        Directory path
    keyword: str
        Keyword
    n: int, optional
        Keep latest n files

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

    Returns
    -------
    None
    """
    import os

    if not isinstance(path, str):
        raise TypeError("Argument 1 must be str ...")
    if not isinstance(keyword, str):
        raise TypeError("Argument 2 must be str ...")
    if not isinstance(n, int):
        raise TypeError("Argument 3 must be int ...")

    to_delete = get_path_files(path=path, keywords=[keyword])[:-n]
    for file in to_delete:
        os.remove(f"{path}/{file}")
        print(f"{path}/{file} deleted ...")


def string_boolean_to_int(boolean_str_rep):
    """
    Return integer from given string boolean. 1 instead of "true"/"True"/"1". Reference from https://docs.python.org/3/distutils/apiref.html#distutils.util.strtobool

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
    """
    from distutils.util import strtobool

    if not isinstance(boolean_str_rep, str):
        raise TypeError("Argument must be str ...")
    int_ = strtobool(string_str_to_str(boolean_str_rep))
    return int_


def string_dlt_to_dlt(dlt_str_rep):
    """
    Return dictionary/list/tuple from given string representation of dictionary/list/tuple

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
    """
    from ast import literal_eval

    if not isinstance(dlt_str_rep, str):
        raise TypeError("Argument must be str ...")
    dlt = literal_eval(dlt_str_rep)
    return dlt


def string_str_to_str(string_str_rep):
    """
    Return string from given string representation of string

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
    """
    if not isinstance(string_str_rep, str):
        raise TypeError("Argument must be str ...")
    str_ = string_str_rep.strip("'\"")
    return str_


if __name__ == "__main__":
    import doctest
    import subprocess

    subprocess.run("mkdir -p test_base_folder", shell=True, executable="/bin/bash")
    subprocess.run("touch test_base_folder/test{1..5}.py", shell=True, executable="/bin/bash")
    doctest.testmod()
    subprocess.run("rm -rf test_base_folder", shell=True, executable="/bin/bash")
