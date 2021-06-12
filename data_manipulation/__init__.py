from data_manipulation import (
    base, beautifulsoup_, django_, geopandas_, pandas_, psycopg2_, pyspark_,
)

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions
