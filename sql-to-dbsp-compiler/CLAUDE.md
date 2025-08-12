# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the SQL-to-DBSP compiler in this repository.

## Key Development Commands

### Building the Compiler

```bash
# Build the compiler (main command)
./build.sh

# Build with current Calcite version (default)
./build.sh -c

# Build with next/unreleased Calcite version
./build.sh -n

# Run all tests (includes building)
./run-tests.sh
```

### Maven Commands

```bash
# Package without running tests
mvn package -DskipTests --no-transfer-progress

# Run unit tests only
mvn test

# Clean build artifacts
mvn clean

# Run specific test class
mvn test -Dtest=PostgresTimestampTests
```

### Rust Dependencies

```bash
# Update Rust dependencies in temp directory
cd temp
cargo update

# Build generated Rust code
cargo build

# Run Rust tests
cargo test
```

## Architecture Overview

### Technology Stack

- **Language**: Java 19+ with Maven build system
- **SQL Parser**: Apache Calcite (version 1.40.0+)
- **Target Language**: Rust (generates DBSP circuits)
- **Testing**: JUnit for unit tests, SQL Logic Tests for comprehensive testing
- **Dependencies**: Jackson for JSON, Janino for code generation

### Compiler Pipeline

The compilation process follows these stages:

1. **SQL Parsing**: Calcite SQL parser generates `SqlNode` IR
2. **Validation & Optimization**: Convert to Calcite `RelNode` representation
3. **Program Assembly**: Creates `CalciteProgram` data structure
4. **DBSP Translation**: `CalciteToDBSPCompiler` converts to `DBSPCircuit`
5. **Circuit Optimization**: `CircuitOptimizer` performs incremental optimizations
6. **Code Generation**: Circuit serialized as Rust using `ToRustString` visitor

### Project Structure

#### Core Directories

- `SQL-compiler/` - Main compiler implementation
  - `src/main/java/org/dbsp/sqlCompiler/` - Core compiler code
    - `circuit/` - DBSP circuit representation and operators
    - `compiler/` - Main compiler infrastructure and metadata
    - `ir/` - Intermediate representation (expressions, types, aggregates)
  - `src/test/` - Unit tests and test utilities
- `simulator/` - DBSP circuit simulator for testing
- `slt/` - SQL Logic Test execution framework
- `temp/` - Generated Rust code output directory (created during builds)
- `lib/` - Rust helper libraries (hashing, readers, SQL value types)
- `multi/` - TODO: what is it for and how is it generated?

#### Key Components

- **Circuit Operators**: Located in `circuit/operator/` - implement DBSP operations
- **Expression System**: In `ir/expression/` - handles SQL expression compilation
- **Type System**: In `ir/type/` - manages SQL to Rust type mappings
- **Aggregation**: In `ir/aggregate/` - handles SQL aggregation functions

## Important Implementation Details

### SQL to DBSP Model

- **Tables**: Become circuit inputs (streaming data sources)
- **Views**: Become circuit outputs (computed results)
- **Incremental Processing**: Supports computing only changes to views when inputs change
- **DBSP Runtime**: Optimized for incremental view maintenance

### Command Line Usage

The compiler is invoked via the `sql-to-dbsp` script:

```bash
# Basic compilation
./sql-to-dbsp input.sql -o output.rs

# Incremental circuit generation
./sql-to-dbsp input.sql -i -o output.rs

# Generate with typed handles
./sql-to-dbsp input.sql --handles -o output.rs

# Generate schema JSON
./sql-to-dbsp input.sql -js schema.json

# Generate circuit visualization
./sql-to-dbsp input.sql --png circuit.png
```

### Key Options

- `-i`: Generate incremental circuit (recommended for streaming)
- `--handles`: Use typed handles instead of Catalog API
- `--ignoreOrder`: Ignore ORDER BY for incrementalization
- `--outputsAreSets`: Ensure outputs don't contain duplicates
- `-O [0,1,2]`: Set optimization level

## Development Workflow

### For Compiler Changes

1. Modify Java source in `SQL-compiler/src/main/java/`
2. Build with `./build.sh`
3. Test changes with `mvn test` or specific test class
4. For Rust code issues: `cd temp && cargo update && cargo build`

### Testing Strategy

#### Unit Tests
- Located in `SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/`
- Each test generates and compiles Rust code
- Run with `mvn test` or specific test with `-Dtest=TestClassName`

#### SQL Logic Tests
- Comprehensive test suite using SqlLogicTest format
- Multiple executors: `dbsp`, `hybrid`, `hsql`, `psql`, `none`
- Run via standalone executable with various options

#### Test Debugging
```bash
# Regenerate Rust code for failed test
mvn test -Dtest=PostgresTimestampTests
cd temp
cargo test
```

### Configuration Files

- `pom.xml` - Maven project configuration with Calcite dependencies
- `build.sh` - Main build script with Calcite version management
- `calcite_version.env` - Environment overrides for Calcite versions
- `run-tests.sh` - Test execution script
- `temp/Cargo.toml` - Rust project configuration for generated code

### Dependencies

- **Apache Calcite**: SQL parser and optimizer infrastructure
- **Jackson**: JSON processing for metadata and serialization
- **JUnit**: Unit testing framework
- **Maven**: Build and dependency management
- **Rust toolchain**: For compiling and testing generated code

### Generated Code Structure

The compiler generates Rust code that:
- Defines input/output data structures
- Implements DBSP circuit functions
- Provides serialization/deserialization support
- Creates typed or catalog-based APIs for data ingestion

### Development Best Practices

- Always run tests after compiler changes
- Use incremental circuits (`-i` flag) for streaming applications
- Generate schema JSON (`-js`) when API contracts matter
- Consider visualization (`--png`) for debugging complex circuits
- Update Rust dependencies when encountering compilation issues