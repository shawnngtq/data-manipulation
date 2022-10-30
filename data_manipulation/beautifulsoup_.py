from typing import Optional

from bs4 import BeautifulSoup


def preprocess(html: str) -> Optional[str]:
    """
    Remove whitespaces and newline characters. Reference: https://stackoverflow.com/questions/23241641/how-to-ignore-empty-lines-while-using-next-sibling-in-beautifulsoup4-in-python

    Parameters
    ----------
    html : str
        html to be cleared

    Examples
    --------
    >>> a = "<html>   <p> Something </p>    </html> "
    >>> preprocess(a)
    '<html><p>Something</p></html>'

    Returns
    -------
    Optional[str]
        cleaned html
    """
    import re

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


def build_soup(
    url: str, features: str = "lxml", to_preprocess: str = True
) -> Optional[BeautifulSoup]:
    """
    Return Beautifulsoup object from given url

    Parameters
    ----------
    url : str
        URL
    features : str, optional
        parser to use
    to_preprocess : str, optional
        to preprocess?

    Examples
    --------
    >>> a = build_soup("https://google.com")
    >>> type(a)
    <class 'bs4.BeautifulSoup'>

    Returns
    -------
    Optional[BeautifulSoup]
        BeautifulSoup parsed by lxml
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
        print(f"response status code: {response.status_code}")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
