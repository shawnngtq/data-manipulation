from . import (
    base, beautifulsoup, django, geopandas, pandas, psycopg2
)
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
