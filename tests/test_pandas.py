"""Tests for data_manipulation.pandas_ (skipped when pandas is absent)."""

import pytest

pd = pytest.importorskip("pandas")

from data_manipulation import pandas_  # noqa: E402


def test_is_running_in_jupyter_is_false():
    assert pandas_.is_running_in_jupyter() is False


def test_add_type_columns():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out = pandas_.add_type_columns(df)
    assert list(out.columns) == ["a", "a_type", "b", "b_type"]
    assert out["a_type"].tolist() == [int, int]
    assert out["b_type"].tolist() == [str, str]


def test_add_type_columns_bad_type():
    with pytest.raises(TypeError):
        pandas_.add_type_columns([1, 2, 3])


def test_split_dataframe():
    df = pd.DataFrame({"id": [1], "a": [2], "b": [3]})
    df1, df2 = pandas_.split_dataframe(df, "id", ["b"])
    assert list(df1.columns) == ["id", "a"]
    assert list(df2.columns) == ["b", "id"]


def test_split_left_merged_dataframe():
    df1 = pd.DataFrame(
        {"key": ["foo", "bar", "baz", "foo", "zen"], "value": [1, 2, 3, 5, 9]}
    )
    df2 = pd.DataFrame(
        {"key": ["foo", "bar", "baz", "foo"], "value": [5, 6, 7, 8]}
    )
    df_both, df_left = pandas_.split_left_merged_dataframe(df1, df2, ["key"])
    assert len(df_both) == 6
    assert df_left["key"].tolist() == ["zen"]


def test_compare_all_list_items():
    out = pandas_.compare_all_list_items([1, 1, 2, 3])
    assert list(out.columns) == ["item1", "item2", "item1_eq_item2"]
    assert len(out) == 6  # C(4, 2)
    assert out.iloc[0]["item1_eq_item2"]


def test_compare_all_list_items_bad_type():
    with pytest.raises(TypeError):
        pandas_.compare_all_list_items(("a", "b"))


def test_dtypes_dictionary():
    df = pd.DataFrame(
        {
            "int_": [0],
            "str_": ["0"],
            "list_": [[]],
            "dict_": [{}],
            "none_": [None],
        }
    )
    result = pandas_.dtypes_dictionary(df)
    assert result[str] == ["str_"]
    assert result[list] == ["list_"]
    assert result[dict] == ["dict_"]
    assert result[type(None)] == ["none_"]
    # The int column comes back as a numpy integer type, not builtin int.
    assert ["int_"] in result.values()


def test_dtypes_dictionary_empty():
    with pytest.raises(ValueError):
        pandas_.dtypes_dictionary(pd.DataFrame())


def test_series_count():
    out = pandas_.series_count(pd.Series([1, 1, 1, 2, 2, 3]))
    assert out["count"].tolist() == [3, 2, 1]
    assert out["percent"].tolist() == pytest.approx([50.0, 100 / 3, 100 / 6])


def test_useless_columns():
    df = pd.DataFrame(
        {"empty": [None, None], "single": ["A", "A"], "normal": ["A", "B"]}
    )
    empty_cols, single_cols = pandas_.useless_columns(df)
    assert empty_cols == ["empty"]
    assert single_cols == ["single"]


def test_compare_dataframes_equal():
    df = pd.DataFrame({"a": [1, 2, 3]})
    result = pandas_.compare_dataframes(df, df.copy())
    assert result["same_length"] is True
    assert result["columns"]["a"]["same"] is True


def test_print_dataframe_overview():
    df = pd.DataFrame({"int_": [1, 1, 2], "str_": ["a", "b", "b"]})
    result = pandas_.print_dataframe_overview(df)
    assert set(result) == {"int_", "str_"}
    assert result["int_"]["unique"] == 2
    assert result["int_"]["null_count"] == 0


def test_to_excel_keep_url(tmp_path):
    pytest.importorskip("xlsxwriter")
    out = tmp_path / "output.xlsx"
    df = pd.DataFrame({"url": ["https://example.com"], "data": ["test"]})
    pandas_.to_excel_keep_url(str(out), df)
    assert out.exists() and out.stat().st_size > 0


def test_ps_aux_dataframe():
    df = pandas_.ps_aux_dataframe("printf 'USER PID\\nalice 1\\n'")
    assert list(df.columns) == ["USER", "PID"]
    assert df.iloc[0]["USER"] == "alice"


def test_ps_aux_dataframe_bad_type():
    with pytest.raises(TypeError):
        pandas_.ps_aux_dataframe(123)
