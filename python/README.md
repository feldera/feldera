# Feldera Python SDK

Feldera Python is the Feldera SDK for Python developers.

## Installation

```bash
pip install feldera
```

### Installing from Github

```bash
pip install git+https://github.com/feldera/feldera#subdirectory=python
```

Similarly, to install from a specific branch:

```bash
$ pip install git+https://github.com/feldera/feldera@{BRANCH_NAME}#subdirectory=python
```

Replace `{BRANCH_NAME}` with the name of the branch you want to install from.

### Installing from Local Directory

If you have cloned the Feldera repo, you can install the python SDK as follows:

```bash
# the Feldera Python SDK is present inside the python/ directory
pip install python/
```

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

## Testing

To run unit tests:

```bash
(cd python && python3 -m unittest)
```

The following command runs end-to-end tests.  You'll need a pipeline
manager running at `http://localhost:8080`.  For the pipeline builder
tests, you'll also need a broker available at `localhost:9092` and
(from the pipelines) `redpanda:19092`.  (To change those locations,
set the environment variables listed in `python/tests/__init__.py`.)

```bash
(cd python/tests && python3 -m pytest .)
```

To run tests from a specific file:

```bash
(cd python/tests && python3 -m unittest ./tests/path-to-file.py)
```

To run the aggregate tests use:

```bash
cd python
PYTHONPATH=`pwd` python3 ./tests/aggregate_tests/main.py
```
