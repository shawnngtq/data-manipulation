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


def parameterize(string_to_clean, separator="-"):
    """
    Convert string to url
    https://coderwall.com/p/nmu4bg/python-parameterize-equivalent-to-rails-parameterize

    Parameters
    ----------
    string_to_clean: str
        String to clean
    separator: str
        Separator

    Examples
    --------
    >>> parameterize("")
    ''
    >>> parameterize(" ")
    ''
    >>> parameterize(" Shawn Ng @ 123")
    'shawn-ng-123'

    >>> parameterize("# Shawn Ng @ 123")
    'shawn-ng-123'

    >>> parameterize("# @ Shawn Ng @ 123")
    'shawn-ng-123'

    Returns
    -------
    str
        Cleaned string
    """
    import re
    import unicodedata

    string_to_clean = string_to_clean.strip().lower()
    parameterized_string = unicodedata.normalize("NFKD", string_to_clean).encode("ASCII", "ignore").decode()
    parameterized_string = re.sub("[^a-zA-Z0-9\-_]+", separator, parameterized_string)

    if separator and separator != "":
        parameterized_string = re.sub("/#{re_separator}{2,}", separator, parameterized_string)
        parameterized_string = re.sub("^#{re_separator}|#{re_separator}$", separator, parameterized_string, re.I)

    '''handle case where symbol is the 1st character'''
    parameterized_string = parameterized_string.lstrip(separator)

    return parameterized_string


if __name__ == "__main__":
    import doctest

    doctest.testmod()
