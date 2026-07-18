# Changelog

## [Unreleased]

### Removed

- **`django_` module** - removed entirely (breaking change). The Django helpers
  (`init_django`, `get_django_countries_dict`, `django_validate_email`,
  `django_validate_url`, `django_validate_phone`) now live in the
  [`django-util`](https://github.com/shawnngtq/django-util) package:
  `django_util.utility` (`init_django`, `get_django_countries_dict`) and
  `django_util.validators` (the `django_validate_*` functions). `data_manipulation`
  no longer depends on Django. This warrants a major version bump on release.

- **`pandas_.clean_none()`** - function has been removed.
  Migrate to pandas built-ins:

  ```python
  df = df.replace(r"^\s*$", np.nan, regex=True)
  df = df.where(pd.notnull(df), None)
  ```

- **`pandas_.config_pandas_display()`** - function has been removed.
  Set display options directly:

  ```python
  pd.set_option("display.max_columns", 500)
  pd.set_option("display.max_colwidth", 500)
  pd.set_option("display.expand_frame_repr", True)
  ```

- **`prometheus_` module** - was an empty placeholder; removed from
  the public API and from `__init__` imports.

### Changed

- **`base.get_none_variation()`** - now returns a `frozenset` of 7
  lowercase canonical strings instead of a 72-item list of case
  permutations. Use `.casefold()` for membership checks:

  ```python
  # before
  if value in get_none_variation():
      ...
  # after
  if isinstance(value, str) and value.casefold() in get_none_variation():
      ...
  ```

- **`pyspark_.group_count()`** - the `percent` column is now computed
  as a native Spark column expression instead of a Python UDF.
  Behaviour is identical; performance improves on large datasets.

- **`pyspark_.column_into_list()`** - raises `ValueError` (with the
  list of available columns) when the requested column does not exist.
  Previously returned `None` silently.

- **`pandas_.print_dataframe_overview()`** - now returns a
  `Dict[str, Dict[str, Any]]` of per-column stats (`unique`,
  `null_count`, `value_counts`). Still prints to stdout as before;
  existing call sites are unaffected.

- **`pandas_.compare_dataframes()`** - now returns a comparison
  summary dict (`same_length`, `df1_length`, `df2_length`, `columns`).
  Still prints to stdout as before; existing call sites are unaffected.

- **`pyspark_` module** - all `print()` calls replaced with
  `logger.info()` / `logger.debug()`. Output is now routed through
  the standard logging pipeline and can be silenced or redirected.

- **`sqlalchemy_.create_sqlalchemy_engine()`** - duplicate
  `if/elif` timeout branches (both setting the same value) consolidated
  into a single condition.

- **`django_.django_validate_phone()`** - now has an explicit
  `return None` on failure instead of a bare `return`.

### Fixed (previous pass - reference)

- `pyspark_.columns_statistics()` - always-true condition in empty-column
  detection (third clause was truthy for any non-empty string).
- `pandas_.dtypes_dictionary()` - `.iteritems()` removed in pandas 2.0;
  replaced with `.items()`.
- Bare `except:` replaced with `except Exception:` in `pandas_` and
  `django_`.
- `base.string_boolean_to_int()` - removed (used `distutils`, dropped
  in Python 3.12).
- `flask_.py` - deleted (was entirely commented-out dead code).
- `beautifulsoup_.preprocess()` - fixed mixed tabs/spaces and invalid
  `\s` escape sequences.
- All modules now guard third-party imports with `try/except`; the
  package imports cleanly even when optional dependencies are missing.
