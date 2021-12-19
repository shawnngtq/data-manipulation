#!/bin/bash

function create_update_docs {
    cd docs
    # Cleanup
    make clean
    rm data_manipulation.rst
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
    rm -r build
    rm -r dist
    rm -r data_manipulation.egg-info
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
