# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Feldera Python SDK in this repository.

## Key Development Commands

### Package Management

This project uses **uv** as the package manager and **pytest** for testing.

```bash
# Install dependencies
uv sync

# Install in development mode
pip install -e .

# Install from local directory
pip install python/

# Install from GitHub
pip install git+https://github.com/feldera/feldera#subdirectory=python
```

### Testing

```bash
# Run all tests
cd python && python3 -m pytest tests/

# Run tests from specific file
cd python && python3 -m pytest ./tests/path-to-file.py

# Run comprehensive test suite
cd python
PYTHONPATH=`pwd` ./tests/run-all-tests.sh

# Run with timeout protection
pytest --timeout=300
```

### Code Quality

```bash
# Lint and format with Ruff
ruff check python/
ruff format python/

# Build documentation
cd docs
sphinx-apidoc -o . ../feldera
make html

# Clean documentation
make clean
```

## Architecture Overview

### Technology Stack

- **Language**: Python 3.10+
- **HTTP Client**: requests library for REST API communication
- **Data Processing**: pandas for DataFrames, numpy for numerical operations
- **Testing**: pytest with timeout support
- **Documentation**: Sphinx with RTD theme
- **Linting/Formatting**: Ruff for code quality
- **Package Manager**: uv for dependency management

### Project Structure

#### Core Directories

- `feldera/` - Main Python package
  - `rest/` - REST API client implementation
  - `pipeline.py` - Main Pipeline class
  - `pipeline_builder.py` - Pipeline construction utilities
  - `output_handler.py` - Output stream handling
- `tests/` - Comprehensive test suite
  - `aggregate_tests/` - SQL aggregation function tests
  - `arithmetic_tests/` - Mathematical operations tests
  - `complex_type_tests/` - Arrays, maps, UDTs tests
  - `variant_tests/` - SQL variant type tests
- `docs/` - Sphinx documentation source

#### Key Components

- **FelderaClient**: Core REST API client for pipeline management
- **Pipeline**: Main interface for pipeline operations and data I/O
- **PipelineBuilder**: Declarative pipeline construction
- **Output Handlers**: Stream processors for pipeline outputs

## Important Implementation Details

### Pipeline Management

- **Incremental Processing**: Supports streaming data with incremental updates
- **SQL Compilation**: Handles CREATE TABLE and CREATE VIEW statements
- **Enterprise Features**: Optional enterprise-only functionality with `@enterprise_only` decorator
- **Runtime Configuration**: Dynamic pipeline configuration management

### Testing Strategy

The SDK uses a sophisticated testing framework optimized for SQL correctness:

#### Shared Test Pipeline Pattern

```python
from tests.shared_test_pipeline import SharedTestPipeline

class TestExample(SharedTestPipeline):
    def test_feature(self):
        """
        CREATE TABLE students(id INT, name STRING);
        CREATE VIEW results AS SELECT * FROM students;
        """
        self.pipeline.start()
        self.pipeline.input_pandas("students", df)
        self.pipeline.wait_for_completion(True)
```

#### Key Testing Principles

- **Inherit from `SharedTestPipeline`** to reduce compilation cycles
- **Define DDLs in docstrings** - all DDLs are combined into single pipeline
- **Use `@enterprise_only`** for enterprise feature tests
- **Runtime config management** with `set_runtime_config()` and `reset_runtime_config()`

### API Design

#### REST Client Architecture

```python
# Core client initialization
from feldera import FelderaClient, Pipeline

client = FelderaClient("http://localhost:8080")
pipeline = client.create_pipeline("my_pipeline")
```

#### Data Integration

- **Pandas Integration**: Native DataFrame input/output support
- **CSV Support**: Direct CSV file processing
- **Streaming**: Real-time data ingestion and output
- **Type Safety**: Automatic type conversion between Python and SQL types

## Development Workflow

### For SDK Changes

1. Modify Python source in `feldera/` package
2. Run linting: `ruff check python/`
3. Run tests: `pytest tests/`
4. Update documentation if needed
5. Test against running Feldera instance (default: `http://localhost:8080`)

### For New Features

1. Add implementation in appropriate module
2. Create comprehensive tests following shared pipeline pattern
3. Add docstring DDLs for any SQL functionality
4. Mark enterprise features with `@enterprise_only`
5. Update documentation with new examples

### Testing Best Practices

- **Environment Setup**: Tests expect Feldera instance at `$FELDERA_BASE_URL` (default: `http://localhost:8080`)
- **Test Organization**: Group related functionality (aggregates, arithmetic, complex types)
- **DDL Management**: Use docstring DDLs in shared test classes
- **Enterprise Testing**: Separate OSS and Enterprise test paths
- **Performance**: Shared pipelines reduce redundant compilation

### Configuration Files

- `pyproject.toml` - Package configuration with dependencies and build settings
- `uv.lock` - Dependency lockfile for reproducible installs
- `docs/conf.py` - Sphinx documentation configuration
- `tests/run-all-tests.sh` - Comprehensive test runner script

### Dependencies

#### Core Dependencies
- `requests` - HTTP client for REST API communication
- `pandas>=2.1.2` - DataFrame operations and data manipulation
- `numpy>=2.2.4` - Numerical computing support
- `typing-extensions` - Enhanced type annotations

#### Development Dependencies
- `pytest>=8.3.5` - Testing framework with timeout support
- `ruff>=0.6.9` - Fast Python linter and formatter
- `sphinx==7.3.7` - Documentation generation
- `kafka-python-ng==2.2.2` - Kafka integration for testing

### Error Handling

- **Pretty Errors**: Automatic error formatting with `pretty-errors`
- **REST API Errors**: Comprehensive error mapping from HTTP responses
- **Type Validation**: Runtime type checking for SQL/Python type conversion
- **Timeout Protection**: Configurable timeouts for long-running operations

### Enterprise Features

- Use `@enterprise_only` decorator for enterprise-specific tests
- Enterprise features are conditionally compiled based on build configuration
- OSS builds automatically skip enterprise DDLs and tests