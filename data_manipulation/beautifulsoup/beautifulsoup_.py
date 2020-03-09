import requests
from bs4 import BeautifulSoup


def build_soup(url):
    """
    Build beautifulsoup with url

    url: str
        URL

    Returns
    -------
    BS4
        Beautifulsoup object of URL
    """
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "lxml")
        return soup
    else:
        print(f"response status code: {response.status_code}")
