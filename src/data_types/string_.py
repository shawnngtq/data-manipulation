def clean_string_representation(string):
    """
    Convert string representation of string to string

    Parameters
    ----------
    string: str

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
    """
    result = None
    if isinstance(string, (str)):
        result = string.strip("'\"")
    return result


def string_boolean_to_int(string):
    """
    Convert string boolean to int
    https://docs.python.org/3/distutils/apiref.html#distutils.util.strtobool

    Parameters
    ----------
    string: str

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
        0 or 1
    """
    result = None
    if isinstance(string, (str)):
        from distutils.util import strtobool
        result = strtobool(clean_string_representation(string))
    return result


def string_to_dict_list(string):
    """
    Convert string representation of dictionary/list to dictionary/list

    Parameters
    ----------
    string: str

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
    """
    result = None
    if isinstance(string, (str)):
        from ast import literal_eval
        result = literal_eval(string)
    return result


if __name__ == "__main__":
    import doctest

    doctest.testmod()
