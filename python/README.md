# Feldera Python SDK

The `feldera` Python package is the Python client for the Feldera HTTP API.

The Python SDK documentation is available at: https://docs.feldera.com/python

## Getting started

### Installation

```bash
uv pip install feldera
```

### Example usage

The Python client interacts with the API server of the Feldera instance.

```python
# File: example.py
from feldera import FelderaClient, PipelineBuilder, Pipeline

# Instantiate client
client = FelderaClient()  # Default: http://localhost:8080 without authentication
# client = FelderaClient(url="https://localhost:8080", api_key="apikey:...", requests_verify="/path/to/tls.crt")

# (Re)create pipeline
name = "example"
sql = """
CREATE TABLE t1 (i1 INT) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;
"""
print("(Re)creating pipeline...")
pipeline = PipelineBuilder(client, name, sql).create_or_replace()
pipeline.start()
print(f"Pipeline status: {pipeline.status()}")
pipeline.pause()
print(f"Pipeline status: {pipeline.status()}")
pipeline.stop(force=True)

# Find existing pipeline
pipeline = Pipeline.get(name, client)
pipeline.start()
print(f"Pipeline status: {pipeline.status()}")
pipeline.stop(force=True)
pipeline.clear_storage()
```

Run using:
```bash
uv run python example.py
```

### Environment variables

Some default parameter values in the Python SDK can be overridden via environment variables.

**Environment variables for `FelderaClient(...)`**

```bash
export FELDERA_HOST="https://localhost:8080"  # Overrides default for `url`
export FELDERA_API_KEY="apikey:..."  # Overrides default for `api_key`

# The following together override default for `requests_verify`
# export FELDERA_TLS_INSECURE="false"  # If set to "1", "true" or "yes" (all case-insensitive), disables TLS certificate verification
# export FELDERA_HTTPS_TLS_CERT="/path/to/tls.crt"  # Custom TLS certificate
```

**Environment variables for `PipelineBuilder(...)`**

```bash
export FELDERA_RUNTIME_VERSION="..."  # Overrides default for `runtime_version`
```

## Development

Development assumes you have cloned the Feldera code repository.

### Installation

```bash
cd python
# Optional: create and activate virtual environment if you don't have one
uv venv
source .venv/bin/activate
# Install in editable mode
uv pip install -e .
```

### Formatting

Formatting requires the `ruff` package: `uv pip install ruff`

```bash
cd python
ruff check
ruff format
```

### Tests

Running the test requires the `pytest` package: `uv pip install pytest`

```bash
# All tests
cd python
uv run python -m pytest tests/

# Specific tests directory
uv run python -m pytest tests/platform/

# Specific test file
uv run python -m pytest tests/platform/test_pipeline_crud.py

# Tip: add argument -x at the end for it to fail fast
```

For further information about the tests, please see `tests/README.md`.

### Documentation

Building documentation requires the `sphinx` package: `uv pip install sphinx`

```bash
cd python/docs
sphinx-apidoc -o . ../feldera
make html
make clean  # Cleanup afterwards
```

### Installation from GitHub

Latest `main` branch:
```bash
uv pip install git+https://github.com/feldera/feldera#subdirectory=python
```

Different branch (replace `BRANCH_NAME`):
```bash
uv pip install git+https://github.com/feldera/feldera@BRANCH_NAME#subdirectory=python
```
