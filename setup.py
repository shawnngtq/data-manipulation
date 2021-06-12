from setuptools import find_packages, setup

import versioneer

LONG_DESCRIPTION = """
Data Manipulation is a Python package providing powerful utility functions. 
It contains many subpackages with utility functions built for popular packages such as Pandas, PySpark and many more.
"""

CLASSIFIERS = [
    "Development Status :: 3 - Alpha",
    "Environment :: Console",
    "Operating System :: OS Independent",
    "Intended Audience :: Science/Research",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Cython",
    "Topic :: Scientific/Engineering",
]

DEPENDENCIES = [
    "beautifulsoup4", "lxml",
    "django",
    "numpy",
    "pandas",
    "IPython", "requests",
    "psycopg2",
    "pyspark",
]

setup(
    author="Shawn Ng",
    author_email="shawn.coding.acc@gmail.com",
    classifiers=CLASSIFIERS,
    cmdclass=versioneer.get_cmdclass(),
    description="Powerful data manipulation",
    install_requires=DEPENDENCIES,
    license="BSD-3",
    long_description=LONG_DESCRIPTION,
    name="data_manipulation",
    packages=find_packages(include=["data_manipulation", "data_manipulation.*"]),
    platforms="any",
    test_suite="nose.collector",
    tests_require=["nose"],
    url="https://github.com/shawnngtq/data-manipulation",
    version=versioneer.get_version(),
)
