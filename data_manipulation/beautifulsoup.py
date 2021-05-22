def build_soup(url):
    """
    Return Beautifulsoup object from given url

    url: str
        URL

    Returns
    -------
    BS4 or None
    """
    import requests
    from bs4 import BeautifulSoup

    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "lxml")
        return soup
    else:
        print(f"response status code: {response.status_code}")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
