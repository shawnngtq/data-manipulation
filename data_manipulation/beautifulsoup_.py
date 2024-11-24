import re
from typing import Optional

from loguru import logger


def preprocess(html: str) -> Optional[str]:
    """Removes whitespaces and newline characters from HTML string.

    Args:
        html (str): HTML string to be cleaned.

    Returns:
        Optional[str]: Cleaned HTML string with normalized whitespace.

    Examples:
        >>> a = "<html>   <p> Something </p>    </html> "
        >>> preprocess(a)
        '<html><p>Something</p></html>'

    Note:
        Reference: https://stackoverflow.com/questions/23241641
    """

    # remove leading and trailing whitespaces
    pattern = re.compile("(^[\s]+)|([\s]+$)", re.MULTILINE)
    html = re.sub(pattern, "", html)
    # convert newlines to spaces, this preserves newline delimiters
    html = re.sub("\n", " ", html)
    # remove whitespaces before opening tags
    html = re.sub("[\s]+<", "<", html)
    # remove whitespaces after closing tags
    html = re.sub(">[\s]+", ">", html)
    return html


def build_soup(url: str, features: str = "lxml", to_preprocess: bool = True):
    """Creates a BeautifulSoup object from a given URL.

    Args:
        url (str): URL to fetch and parse.
        features (str, optional): Parser to use. Defaults to "lxml".
        to_preprocess (bool, optional): Whether to preprocess the HTML. Defaults to True.

    Returns:
        Optional[BeautifulSoup]: Parsed BeautifulSoup object, or None if request fails.

    Examples:
        >>> a = build_soup("https://google.com")
        >>> type(a)
        <class 'bs4.BeautifulSoup'>

    Note:
        Requires requests and beautifulsoup4 packages.
    """
    import requests
    from bs4 import BeautifulSoup

    response = requests.get(url)
    if response.status_code == 200:
        if to_preprocess:
            soup = BeautifulSoup(preprocess(response.text), features=features)
        else:
            soup = BeautifulSoup(response.text, features=features)
        return soup
    else:
        logger.error(f"response status code: {response.status_code}")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
