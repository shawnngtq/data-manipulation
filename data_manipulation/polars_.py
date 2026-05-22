from __future__ import annotations

import hashlib

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    pd = None
    HAS_PANDAS = False

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

try:
    from pandas.testing import assert_frame_equal
    HAS_PANDAS_TESTING = True
except ImportError:
    assert_frame_equal = None
    HAS_PANDAS_TESTING = False


def are_dataframes_equal(
    polars_df: pl.DataFrame,
    pandas_df: pd.DataFrame,
    *,
    check_dtype: bool = False,
    check_order: bool = True,
) -> bool:
    """
    Comprehensive comparison of Polars and Pandas DataFrames.

    Parameters:
    - polars_df: polars.DataFrame
    - pandas_df: pandas.DataFrame
    - check_dtype: bool, whether to check dtypes (default False)
    - check_order: bool, whether column order matters (default True)

    Returns:
    - bool: True if DataFrames are exactly equal
    """
    # --------------------------
    # 1. Quick Shape Check (fastest)
    # --------------------------
    if polars_df.shape != pandas_df.shape:
        return False

    # --------------------------
    # 2. Hash Comparison (fast for large, equal DFs)
    # --------------------------
    def get_hash(df):
        if isinstance(df, pl.DataFrame):
            return hashlib.sha256(df.to_pandas().to_string().encode()).hexdigest()
        return hashlib.sha256(df.to_string().encode()).hexdigest()

    hash_equal = get_hash(polars_df) == get_hash(pandas_df)
    if hash_equal:
        return True

    # --------------------------
    # 3. Full Content Comparison (most thorough)
    # --------------------------
    try:
        polars_pd = polars_df.to_pandas()

        # Handle column order if needed
        if not check_order:
            # Ensure same columns exist
            if set(polars_pd.columns) != set(pandas_df.columns):
                return False
            # Reorder pandas columns to match polars
            pandas_df = pandas_df[polars_pd.columns]

        assert_frame_equal(
            polars_pd,
            pandas_df,
            check_dtype=check_dtype,
            check_exact=True,
            check_like=not check_order,
        )
        return True
    except (AssertionError, ValueError):
        return False
