# Python Testing

## Overview

This directory contains the main integration tests for feldera.
Think about where to add new tests when covering any new features or bug fixes.
The tests are organized into these categories:

1. **Runtime Tests**: These tests validate the core functionality of feldera's runtime
   (e.g., adapters, ingress, egress, query execution, checkpointing, object store sync,
   and result correctness).
   There are three types of runtime tests:
     - `runtime` folder: These are tests that use `pytest`, `unittest` to run. See advice
        below on how to reduce redundant compilation cycles.
     - `runtime_aggtest` folder: These tests use a custom testing framework designed for
        batching many SQL programs into a single pipeline.
     - `workloads` These tests are designed to run larger, standardized workloads
        like TPC-H or TPC-DS. They can be used for benchmarking as well.
2. **Platform Tests**: These tests focus on testing the platform-specific features of feldera.
    They ensure that REST APIs, configurations, deployment work as expected and the platform
    is robust w.r.g. to scale etc.
    Sometimes there is obvious overlap with runtime tests, but platform tests are more
    focused on the platform layer and should not need to test any runtime features in
    depth.
    Platform tests are located in the `platform` folder: These are standard unit tests that
    use the `pytest` framework.

## Running `pytest` Tests

To run unit tests:

```bash
cd python && python3 -m pytest tests/
```

- This will detect and run all test files that match the pattern `test_*.py` or
  `*_test.py`.
- By default, the tests expect a running Feldera instance at `http://localhost:8080`.
  To override the default endpoint, set the `FELDERA_HOST` environment variable.

To run tests from a specific file:

```bash
(cd python && python3 -m pytest ./tests/runtime/path-to-file.py)
```

To run a specific test:

```bash
uv run python -m pytest tests/platform/test_shared_pipeline.py::TestPipeline::test_adhoc_query_hash -v
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


## Running `runtime_aggtest` Tests

These tests run with a different testing framework. To execute them, use:

```bash
cd python
PYTHONPATH=`pwd` ./tests/runtime_aggtest/run.sh
```
