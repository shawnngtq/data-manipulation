# from . import (
#     base, beautifulsoup, django, geopandas, pandas, psycopg2, pyspark,
# )
from data_manipulation.base import base_
from data_manipulation.beautifulsoup import beautifulsoup_
from data_manipulation.django import django_
from data_manipulation.geopandas import geopandas_
from data_manipulation.pandas import pandas_
from data_manipulation.psycopg2 import psycopg2_
from data_manipulation.pyspark import pyspark_

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
