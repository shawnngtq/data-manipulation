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

function setup_versioneer {
    # Setup versioneer
    versioneer install
}

function cleanup {
    rm -rf build
    rm -rf dist
    rm -rf data_manipulation.egg-info
    rm -rf data_manipulation-*
    # Build distribution
    python setup.py sdist bdist_wheel
}

function upload {
    # Upload to Test PyPi
    # python3 -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
    # Upload to PyPi
    twine upload dist/*
}

"$@"
exit 0
