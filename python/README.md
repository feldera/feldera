# Feldera Python SDK 

Feldera Python is the Feldera SDK for Python developers.

## Installation

```bash
pip install git+https://github.com/feldera/feldera#subdirectory=python
```

Similarly, to install from a specific branch:

```bash
$ pip install git+https://github.com/feldera/feldera@{BRANCH_NAME}#subdirectory=python
```

Replace `{BRANCH_NAME}` with the name of the branch you want to install from.

Checkout the docs [here](./feldera/__init__.py) for an example on how to use the SDK.

## Documentation

To build the html documentation run: 

Ensure that you have sphinx installed. If not, install it using `pip install sphinx`.

Then run the following commands:

```bash
cd docs
sphinx-apidoc -o . ../feldera
make html
```

To clean the build, run `make clean`.