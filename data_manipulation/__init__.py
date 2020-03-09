from . import beautifulsoup
from . import data_types
from . import geopandas
from . import pandas
from . import psycopg2
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions
