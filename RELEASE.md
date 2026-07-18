# Release Guide

This guide is for maintainers publishing documentation to GitHub Pages and
package artifacts to PyPI.

## Preconditions

- Work from a clean `master` branch.
- Release versions are derived from git tags by `setuptools-scm`.
- Never build or upload release artifacts from a dirty worktree.
- Keep PyPI credentials out of git.

Check the release state:

```bash
git checkout master
git pull --ff-only origin master
git status
```

`git status` should report no changes before continuing.

## Tag The Release

Use the next release version, for example `0.50`:

```bash
git tag -a 0.50 -m "Release 0.50"
git push origin master
git push origin 0.50
```

The tag must point at the exact commit being released.

## Publish Documentation

Both commands require an environment that provides `mkdocs`, `mkdocs-material`, and
`mkdocstrings-python` (for example a virtualenv, conda env, or pixi shell). Activate that
environment before running them.

Build the MkDocs documentation (local preview only):

```bash
./setup.sh create_update_docs
```

Deploy to GitHub Pages:

```bash
./setup.sh deploy_docs
```

This publishes the contents of `site/` to the `gh-pages` branch.

**Note - documentation and PyPI releases are independent.**

- **Tags are only for PyPI releases.** `setuptools-scm` derives the package
  version from git tags. Create a tag only when the shipped `data_manipulation/`
  package changes; tooling- or docs-only changes need no tag.
- **Docs deploy without a tag.** `deploy_docs` publishes current `master` to the
  separate `gh-pages` branch, independent of any tag or PyPI release.
- **Commit and push doc/source changes to `master` before building.** The site
  renders the working tree, and `create_update_docs` writes only to the
  gitignored `site/` directory - no tracked files change.
- **When cutting a release, build docs after tagging** so the published site
  matches the released commit.

## Publish To PyPI

Build and validate the distribution artifacts:

```bash
./setup.sh cleanup
```

Upload the artifacts:

```bash
./setup.sh upload
```

When prompted by `twine`, enter the PyPI API token. The token should start with
`pypi-`. It will not be displayed while typing or pasting.

## Verify The Release

Install the published package in a fresh environment and confirm the version:

```bash
python3 -m pip install --upgrade data-manipulation
python3 -c "import data_manipulation; print(data_manipulation.__version__)"
```

## Security Notes

- Do not commit PyPI API tokens, passwords, or `.pypirc` files with credentials.
- Do not add tokens to shell history as command-line arguments.
- Prefer entering the PyPI token at the `twine` prompt or using a local secret
  store.
- If a token is accidentally committed or exposed, revoke it in PyPI immediately
  and create a new one.
