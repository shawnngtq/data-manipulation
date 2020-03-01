from setuptools import find_packages, setup

import versioneer

LONG_DESCRIPTION = """
Data Manipulation is a Python library for making it easier to manipulate data using popular libraries such as Pandas & PySpark.
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
    "Programming Language :: Cython",
    "Topic :: Scientific/Engineering",
]

DEPENDENCIES = [
    "numpy", "pandas", "IPython", "psycopg2",
]

setup(
    author="Shawn Ng",
    author_email="shawn.coding.acc@gmail.com",
    classifiers=CLASSIFIERS,
    cmdclass=versioneer.get_cmdclass(),
    description="Powerful data manipulation functions",
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
