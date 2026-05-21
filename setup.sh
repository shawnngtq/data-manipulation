#!/bin/bash

function create_update_docs {
    cd docs
    # Cleanup
    make clean
    # rm data_manipulation.rst
    # rm modules.rst
    # Generate rst
    sphinx-apidoc -o . ../data_manipulation/
    # Generate html
    make html
    cd ..
}

function cleanup {
    rm -rf build
    rm -rf dist
    rm -rf data_manipulation.egg-info
    rm -rf data_manipulation-*
    # Build distribution
    python -m build
    python -m twine check --strict dist/*
}

function upload {
    # Upload to Test PyPi
    # python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
    # Upload to PyPi
    python -m twine upload dist/*
}

"$@"
exit 0
