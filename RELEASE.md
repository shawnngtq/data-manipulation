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

Build the MkDocs documentation:

```bash
mkdocs build
```

Publish the generated HTML to GitHub Pages:

```bash
mkdocs gh-deploy
```

This publishes the contents of `site/` to the `gh-pages` branch.

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
