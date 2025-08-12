# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the crates directory in this repository.

## Overview

The `crates/` directory contains all Rust crates that make up the Feldera platform. This is a workspace-based project where each crate serves a specific purpose in the overall architecture, from core computational engines to I/O adapters and development tools.

## Crate Descriptions

### Core Engine Crates

**`dbsp/`** - The Database Stream Processor is the heart of Feldera's computational engine. It provides incremental computation capabilities where changes propagate in time proportional to the size of the change rather than the dataset. Contains operators for filtering, mapping, joining, aggregating, and advanced streaming operations with support for multi-threaded execution and persistent storage.

**`sqllib/`** - Runtime support library providing SQL function implementations for the compiled circuits. Includes aggregate functions (SUM, COUNT, AVG), scalar functions (string manipulation, date/time operations), type conversions, and null handling following SQL standard semantics. Essential for SQL compatibility in the DBSP computational model.

**`feldera-types/`** - Shared type definitions and configuration structures used across the entire platform. Provides pipeline configuration types, transport configurations, data format schemas, error types, and validation frameworks. Serves as the foundational type system ensuring consistency across all Feldera components.

### Service and Management Crates

**`pipeline-manager/`** - HTTP API service for managing the lifecycle of data pipelines. Provides RESTful endpoints for creating, configuring, starting, stopping, and monitoring pipelines. Integrates with PostgreSQL for persistence, handles authentication, and orchestrates the compilation and deployment of SQL programs to DBSP circuits.

**`rest-api/`** - Type definitions and OpenAPI specification generation for the Feldera REST API. Automatically generates machine-readable API specifications from Rust types, ensuring consistency between server implementation and client SDKs. Includes comprehensive request/response schemas and validation rules.

### I/O and Integration Crates

**`adapters/`** - Comprehensive I/O framework providing input and output adapters for DBSP circuits. Supports multiple transport protocols (Kafka, HTTP, File, Redis, S3) and data formats (JSON, CSV, Avro, Parquet). Includes integrated connectors for databases like PostgreSQL and data lake formats like Delta Lake with fault-tolerant processing and automatic retry logic.

**`adapterlib/`** - Foundational abstractions and utilities for building I/O adapters. Provides generic transport traits, format processing abstractions, circuit catalog interfaces for runtime introspection, and comprehensive error handling frameworks. Enables consistent adapter implementation across different data sources and sinks.

**`iceberg/`** - Apache Iceberg table format integration enabling Feldera to work with modern data lake architectures. Supports schema evolution, time travel queries, and efficient data partitioning. Includes S3 and cloud storage integration with optimized reading patterns for large analytic datasets.

### Storage and Persistence Crates

**`storage/`** - Pluggable storage abstraction layer supporting multiple backends including memory-based storage for testing and POSIX I/O for production deployments. Provides async file operations, block-level caching, buffer management, and error recovery mechanisms optimized for DBSP's access patterns.

### Mathematical and Type System Crates

**`fxp/`** - High-precision fixed-point arithmetic library for financial and decimal computations. Provides exact decimal arithmetic without floating-point errors, SQL DECIMAL type compatibility, and DBSP integration with zero-copy serialization. Supports both compile-time fixed scales and dynamic precision for flexible numeric processing.

### Benchmarking and Testing Crates

**`nexmark/`** - Industry-standard streaming benchmark suite implementing the NEXMark benchmark queries. Generates realistic auction data with configurable rates and distributions, implements all 22 standard benchmark queries, and provides performance measurement tools for validating DBSP's streaming capabilities.

**`datagen/`** - Test data generation utilities providing realistic datasets for testing and benchmarking. Supports configurable data distributions, correlated data generation, high-throughput batch generation, and deterministic seeded generation for reproducible testing scenarios.

### Development and Tooling Crates

**`fda/`** - Feldera Development Assistant providing CLI tools and interactive development environment. Includes command-line utilities for common development tasks, interactive shell for exploratory development, benchmarking tools, and API specification validation. Serves as the primary development companion tool.

**`ir/`** - Multi-level intermediate representation system for SQL compilation pipeline. Provides High-level IR (HIR) close to SQL structure, Mid-level IR (MIR) for optimization, and Low-level IR (LIR) for code generation. Includes comprehensive analysis frameworks and transformation passes for SQL program compilation.

## Development Workflow

### Building All Crates

```bash
# Build all crates
cargo build --workspace

# Test all crates
cargo test --workspace

# Test documentation (limit threads to prevent OOM)
cargo test --doc -- --test-threads 12

# Check all crates
cargo check --workspace

# Build specific crate
cargo build -p <crate-name>
```

### Workspace Management

```bash
# List all workspace members
cargo metadata --format-version=1 | jq '.workspace_members'

# Run command on all workspace members
cargo workspaces exec cargo check

# Update dependencies across workspace
cargo update
```

### Cross-Crate Dependencies

The crates form a dependency graph where:
- **Core crates** (`dbsp`, `feldera-types`) are dependencies for most other crates
- **Service crates** (`pipeline-manager`, `adapters`) depend on core and utility crates
- **Tool crates** (`fda`, `nexmark`) typically depend on multiple core crates
- **Library crates** (`sqllib`, `adapterlib`) provide functionality to higher-level crates

### Testing Strategy

- **Unit Tests**: Each crate contains comprehensive unit tests
- **Integration Tests**: Cross-crate integration testing
- **Workspace Tests**: Full workspace testing for compatibility
- **Benchmark Tests**: Performance validation across crates

## Best Practices

### Crate Organization
- Each crate has a single, well-defined responsibility
- Dependencies flow from higher-level to lower-level crates
- Shared types and utilities are extracted to common crates
- Feature flags control optional functionality and integrations

### Development Guidelines
- Follow consistent coding patterns across crates
- Use workspace-level dependency management
- Maintain comprehensive documentation for each crate
- Write tests that work both in isolation and as part of the workspace

### Performance Considerations
- Core computational crates (`dbsp`, `sqllib`) are highly optimized
- I/O crates (`adapters`, `storage`) focus on throughput and efficiency
- Tool crates prioritize developer experience over raw performance
- Benchmark crates provide performance validation and regression detection

This workspace architecture enables modular development while maintaining consistency and performance across the entire Feldera platform.