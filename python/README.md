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

## Documentation

The Python SDK documentation is available at
[Feldera Python SDK Docs](https://docs.feldera.com/python).

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
cd python && python3 -m pytest tests/
```

- This will detect and run all test files that match the pattern `test_*.py` or
  `*_test.py`.
- By default, the tests expect a running Feldera instance at `http://localhost:8080`.
  To override the default endpoint, set the `FELDERA_BASE_URL` environment variable.

To run tests from a specific file:

```bash
(cd python && python3 -m pytest ./tests/path-to-file.py)
```

#### Running Aggregate Tests

The aggregate tests validate end-to-end correctness of SQL functionality.
To run the aggregate tests use:

```bash
cd python
PYTHONPATH=`pwd` python3 ./tests/aggregate_tests/main.py
```

### Reducing Compilation Cycles

To reduce redundant compilation cycles during testing:

* **Inherit from `SharedTestPipeline`** instead of `unittest.TestCase`.
* **Define DDLs** (e.g., `CREATE TABLE`, `CREATE VIEW`) in the **docstring** of each test method.
  * All DDLs from all test functions in the class are combined and compiled into a single pipeline.
  * If a table or view is already defined in one test, it can be used directly in others without redefinition.
  * Ensure that all table and view names are unique within the class.
* Use `@enterprise_only` on tests that require Enterprise features. Their DDLs will be skipped on OSS builds.
* Use `self.set_runtime_config(...)` to override the default pipeline config.
  * Reset it at the end using `self.reset_runtime_config()`.
* Access the shared pipeline via `self.pipeline`.

#### Example

```python
from tests.shared_test_pipeline import SharedTestPipeline

class TestAverage(SharedTestPipeline):
    def test_average(self):
        """
        CREATE TABLE students(id INT, name STRING);
        CREATE MATERIALIZED VIEW v AS SELECT * FROM students;
        """
        ...
        self.pipeline.start()
        self.pipeline.input_pandas("students", df)
        self.pipeline.wait_for_completion(True)
        ...
```

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
