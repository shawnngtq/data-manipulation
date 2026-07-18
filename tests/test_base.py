"""Tests for data_manipulation.base (pure stdlib — always run)."""

import pytest

from data_manipulation import base


class TestCleanString:
    @pytest.mark.parametrize(
        "raw,kwargs,expected",
        [
            (" sHawn  tesT ", {}, "SHAWN TEST"),
            ("shawn ( te  st )", {}, "SHAWN ( TE ST )"),
            ("shawn ( te  st )", {"remove_parenthesis": True}, "SHAWN"),
            ("shawn [ te  st ]", {"remove_brackets": True}, "SHAWN"),
        ],
    )
    def test_cleans(self, raw, kwargs, expected):
        assert base.clean_string(raw, **kwargs) == expected

    @pytest.mark.parametrize("bad", ["", None])
    def test_rejects_empty(self, bad):
        with pytest.raises(ValueError):
            base.clean_string(bad)


def test_get_string_case_combination():
    assert base.get_string_case_combination("abc") == [
        "ABC",
        "ABc",
        "AbC",
        "Abc",
        "aBC",
        "aBc",
        "abC",
        "abc",
    ]


def test_get_string_case_combination_empty():
    with pytest.raises(ValueError):
        base.get_string_case_combination("")


@pytest.mark.parametrize(
    "value,present",
    [("none", True), ("NONE", True), ("NULL", True), ("valid", False)],
)
def test_get_none_variation(value, present):
    assert (value.casefold() in base.get_none_variation()) is present


def test_country_variations_are_inverse_pairs():
    names = base.get_country_name_variation()
    codes = base.get_country_code_variation()
    assert names["UNITED STATES"] == "US"
    assert "UNITED STATES" in codes["US"]


@pytest.mark.parametrize(
    "value,expected",
    [
        (["a", "none"], ["a"]),
        (("a", "none"), ("a",)),
        (["a", "", None, "NaN"], ["a"]),
    ],
)
def test_list_tuple_without_none(value, expected):
    assert base.list_tuple_without_none(value) == expected


def test_list_tuple_without_none_bad_type():
    with pytest.raises(TypeError):
        base.list_tuple_without_none({"a": 1})


def test_delete_list_indices_mutates_in_place():
    values = [0, 1, 2, 3, 4]
    assert base.delete_list_indices(values, [1, 3]) is None
    assert values == [0, 2, 4]


def test_get_path_files(sample_files):
    assert base.get_path_files(sample_files, ["py"]) == [
        "test1.py",
        "test2.py",
        "test3.py",
        "test4.py",
        "test5.py",
    ]


def test_get_path_files_missing_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        base.get_path_files(tmp_path / "nope", ["py"])


def test_remove_path_file_keeps_n_newest(sample_files):
    base.remove_path_file(sample_files, ".py", n=2)
    remaining = sorted(p.name for p in sample_files.iterdir())
    assert remaining == ["test4.py", "test5.py"]


def test_list_to_file(tmp_path):
    out = tmp_path / "out.txt"
    base.list_to_file(out, [1, 2, 3])
    assert out.read_text() == "1\n2\n3\n"


def test_create_encode_url():
    url = base.create_encode_url("https://example.com/path", {"a": 1, "b": 2})
    assert url == "https://example.com/path?a=1&b=2"


def test_create_encode_url_invalid():
    with pytest.raises(ValueError):
        base.create_encode_url("not a url")


def test_parse_ps_aux_parses_columns():
    # Feed a deterministic command instead of relying on real `ps` output.
    rows = base.parse_ps_aux("printf 'USER PID CMD\\nalice 1 top\\n'")
    assert rows[0] == ["USER", "PID", "CMD"]
    assert rows[1] == ["alice", "1", "top"]
