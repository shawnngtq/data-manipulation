"""Tests for data_manipulation.beautifulsoup_.

preprocess is pure regex (always runs); build_soup performs HTTP and is mocked
under the integration marker.
"""

import pytest

from data_manipulation import beautifulsoup_


def test_preprocess_normalizes_whitespace():
    html = "<html>   <p> Something </p>    </html> "
    assert beautifulsoup_.preprocess(html) == "<html><p>Something</p></html>"


@pytest.mark.integration
def test_build_soup_mocked(mocker):
    pytest.importorskip("bs4")
    requests = pytest.importorskip("requests")

    fake_response = mocker.Mock()
    fake_response.text = "<html>  <p> hi </p> </html>"
    fake_response.raise_for_status = mocker.Mock()
    mocker.patch.object(requests.Session, "get", return_value=fake_response)

    soup = beautifulsoup_.build_soup(
        "https://example.com", features="html.parser"
    )
    assert soup is not None
    assert "hi" in soup.get_text()


def test_build_soup_rejects_bad_url():
    # build_soup imports requests/bs4 at the top of its body, before the URL
    # check runs, so those must be importable to reach the ValueError.
    pytest.importorskip("bs4")
    pytest.importorskip("requests")
    with pytest.raises(ValueError):
        beautifulsoup_.build_soup("ftp://example.com")
