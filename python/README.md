# Feldera Python SDK

Feldera Python is the Feldera SDK for Python developers.

## Installation

```bash
uv pip install feldera
```

### Installing from Github

```bash
uv pip install git+https://github.com/feldera/feldera#subdirectory=python
```

Similarly, to install from a specific branch:

```bash
uv pip install git+https://github.com/feldera/feldera@{BRANCH_NAME}#subdirectory=python
```

Replace `{BRANCH_NAME}` with the name of the branch you want to install from.

### Installing from Local Directory

If you have cloned the Feldera repo, you can install the python SDK as follows:

```bash
# the Feldera Python SDK is present inside the python/ directory
cd python
# If you don't have a virtual environment, create one
uv venv
source .venv/activate
# Install the SDK in editable mode
uv pip install .
```

You also have to install the `pytest` module:

```bash
python3 -m pip install pytest
```

## Documentation

The Python SDK documentation is available at
[Feldera Python SDK Docs](https://docs.feldera.com/python).

To build the html documentation run:

Ensure that you have sphinx installed. If not, install it using `uv pip install sphinx`.

Then run the following commands:

```bash
cd docs
sphinx-apidoc -o . ../feldera
make html
```

To clean the build, run `make clean`.

## Linting and formatting

Use [Ruff] to run the lint checks that will be executed by the
precommit hook when a PR is submitted:

```bash
ruff check python/
```

To reformat the code in the same way as the precommit hook:

```bash
ruff format
```

[Ruff]: https://github.com/astral-sh/ruff
