import re
from typing import TYPE_CHECKING, Optional

from loguru import logger

if TYPE_CHECKING:
    from bs4 import BeautifulSoup

# Constants
DEFAULT_TIMEOUT = 10
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


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


def build_soup(
    url: str,
    features: str = "lxml",
    to_preprocess: bool = True,
    timeout: int = DEFAULT_TIMEOUT,
    headers: Optional[dict] = None,
) -> "Optional[BeautifulSoup]":
    """Creates a BeautifulSoup object from a given URL.

    Args:
        url (str): URL to fetch and parse.
        features (str, optional): Parser to use. Defaults to "lxml".
        to_preprocess (bool, optional): Whether to preprocess the HTML. Defaults to True.
        timeout (int, optional): Request timeout in seconds. Defaults to 10.
        headers (Optional[dict], optional): Custom headers for the request. Defaults to None.

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

    if not url or not url.strip():
        raise ValueError("URL cannot be empty")

    if not url.startswith(("http://", "https://")):
        raise ValueError("URL must start with http:// or https://")

    request_headers = headers or DEFAULT_HEADERS

    try:
        with requests.Session() as session:
            response = session.get(url, headers=request_headers, timeout=timeout)
            response.raise_for_status()

            html_content = preprocess(response.text) if to_preprocess else response.text
            return BeautifulSoup(html_content, features=features)

    except requests.RequestException as e:
        logger.error(f"Failed to fetch URL {url}: {str(e)}")
        return None


if __name__ == "__main__":
    import doctest

    doctest.testmod()
