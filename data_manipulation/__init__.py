from data_manipulation import (
    base, beautifulsoup, django, geopandas, pandas, psycopg2, pyspark,
)

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions
