> Important! When `SECTION:some/path/CLAUDE.md START` HTML comment tag is encountered in this file, it means that the content within this tag applies to `some/path/` directory.

## Context: CLAUDE.md
<!-- SECTION:CLAUDE.md START -->
# CLAUDE.md

This and any nested CLAUDE.md files provide guidance to Claude Code (claude.ai/code) when working with the Feldera repository.

## Repository Overview

Feldera is an **incremental view maintenance (IVM) system** built around **DBSP (Database Stream Processor)**, a computational model that enables true incremental computation. The repository implements a complete platform where SQL queries are compiled into streaming circuits that process only changes rather than recomputing entire datasets, enabling high-performance analytics that scale with change volume rather than data size.

## Core Technical Architecture

### **DBSP: The Computational Foundation**

At the heart of Feldera lies DBSP, a computational model for incremental stream processing:

- **Change Propagation**: Processes streams of changes (insertions, deletions, updates) rather than full datasets
- **Algebraic Operators**: Implements relational algebra operators (join, aggregate, filter) that work incrementally
- **Nested Relational Model**: Supports complex data types and nested queries through compositional operators
- **Circuit Model**: SQL queries compile to circuits of interconnected DBSP operators that maintain incremental state

### **SQL-to-Circuit Compilation Pipeline**

The transformation from declarative SQL to executable circuits:

1. **SQL Parsing & Validation** (Apache Calcite in Java)
2. **Incremental Conversion** (Automatic IVM transformation)
3. **Circuit Generation** (DBSP operator graph construction)
4. **Code Generation** (Rust implementation of circuits)
5. **Runtime Integration** (Circuit deployment and execution)

### **Multi-Language Implementation Strategy**

Each language serves a specific role in the IVM implementation:

- **Rust**: High-performance DBSP runtime with zero-cost abstractions and memory safety
- **Java**: SQL compilation leveraging Apache Calcite for parsing, optimization, and incremental query planning
- **Python**: User-facing APIs and data science integration with type safety and ergonomic interfaces
- **TypeScript**: Web interface for visual pipeline development and real-time monitoring

## Repository Architecture: IVM Implementation

This repository implements the complete Feldera IVM system as interconnected components that transform SQL into high-performance incremental computation:

### **SQL-to-DBSP Transformation Pipeline**

1. **Declarative Input** → SQL queries with complex joins, aggregates, and window functions
2. **Incremental Planning** → Apache Calcite-based transformation to change-oriented operations
3. **Circuit Construction** → DBSP operator graphs with dataflow connections and state management
4. **Code Generation** → Rust implementations optimized for incremental execution
5. **Runtime Deployment** → Multi-threaded circuit execution with fault tolerance

### **IVM-Centered Component Integration**

Each repository component serves the incremental computation model:

- **`crates/dbsp/`**: Core IVM engine implementing 50+ incremental operators
- **`sql-to-dbsp-compiler/`**: SQL→DBSP circuit transformation with IVM optimizations
- **`crates/pipeline-manager/`**: Circuit lifecycle management and deployment orchestration
- **`crates/adapters/`**: Change stream I/O for incremental data ingestion/emission
- **`python/`**: IVM-aware APIs for programmatic circuit management

## Component Ecosystem

### **DBSP Runtime Engine (`crates/`)**
The incremental computation runtime implemented as a Rust workspace with 15+ specialized crates:

#### **DBSP Computational Core**
- **`dbsp/`**: The core IVM engine implementing the DBSP computational model with incremental operators (map, filter, join, aggregate), circuit execution runtime, multi-threaded scheduling with work-stealing, and persistent state management for large-scale incremental computation
- **`sqllib/`**: Runtime library providing SQL function implementations optimized for incremental execution, including incremental aggregates (SUM, COUNT, AVG with +/- updates), scalar functions with null propagation, and DBSP-compatible type conversions

#### **Circuit Management**
- **`pipeline-manager/`**: HTTP service orchestrating the complete IVM workflow: SQL compilation requests, DBSP circuit deployment, runtime circuit lifecycle management, and integration with storage/networking for production incremental computation systems
- **`rest-api/`**: OpenAPI specifications for IVM system management ensuring type-safe client-server communication for circuit operations, pipeline configuration, and incremental computation monitoring

#### **Incremental I/O Framework**
- **`adapters/`**: Change-oriented I/O system enabling incremental computation across external systems. Implements change data capture (CDC) for databases, streaming connectors (Kafka) with exactly-once semantics, and delta-oriented file processing (Parquet, Delta Lake) optimized for IVM workflows
- **`adapterlib/`**: Core abstractions for building IVM-compatible adapters including change stream protocols, incremental batch processing, circuit integration APIs, and fault tolerance mechanisms for long-running incremental computations

#### **IVM Infrastructure**
- **`feldera-types/`**: Shared type system for the IVM platform including circuit configuration types, incremental data structures, change stream representations, and error handling across the SQL-to-DBSP compilation and runtime execution pipeline
- **`fxp/`**: High-precision decimal arithmetic with exact incremental computation properties, supporting financial applications where floating-point errors would compound across incremental updates
- **`storage/`**: Pluggable storage backend supporting incremental computation persistence, including memory-optimized storage for development and disk-based storage for production IVM deployments with efficient delta management
- **`nexmark/`**: Streaming analytics benchmark suite validating IVM performance against industry standards, with 22 complex queries testing incremental joins, aggregations, and window operations

### **SQL-to-DBSP Compiler (`sql-to-dbsp-compiler/`)**
Java-based compilation system implementing the core IVM transformation from declarative SQL to incremental DBSP circuits:

#### **IVM Compilation Stages**
1. **SQL Parsing**: Apache Calcite-based parsing with comprehensive SQL dialect support
2. **Incremental Planning**: Automatic transformation of SQL operations into change-propagating equivalents (e.g., JOIN → incremental join with state management)
3. **DBSP Lowering**: Conversion to DBSP operator graphs with explicit state management and change propagation semantics
4. **Circuit Optimization**: DBSP-specific optimizations including operator fusion, state minimization, and parallelization opportunities
5. **Rust Code Generation**: Production of type-safe Rust implementations with zero-cost abstractions

#### **IVM-Specific Features**
- **Automatic Delta Computation**: Transforms standard SQL semantics into change-oriented operations maintaining mathematical correctness
- **Nested Query Support**: Handles complex subqueries through DBSP's nested relational model with incremental correlation
- **Recursive Query Compilation**: Implements SQL recursion using DBSP's fixed-point iteration with incremental convergence detection
- **Type System Integration**: Maps SQL types to Rust representations optimized for incremental computation and serialization

### **IVM Management SDK (`python/`)**
Python SDK providing programmatic control over the complete IVM system lifecycle:

#### **Circuit Management Capabilities**
- **Pipeline Orchestration**: Create, deploy, and monitor DBSP circuits through high-level Python APIs with automatic SQL-to-circuit compilation
- **Incremental Data Integration**: Pandas DataFrame integration optimized for IVM workflows, supporting efficient change detection and delta-oriented data loading
- **Circuit Testing Framework**: Shared test patterns enabling rapid iteration on IVM logic with optimized compilation cycles and deterministic change stream generation
- **Enterprise IVM Features**: Advanced incremental computation features (circuit optimization, distributed execution, persistent state management) with feature flag controls

#### **IVM-Aware Developer Experience**
- **Circuit Type Safety**: Full type annotations reflecting the compiled DBSP circuit structure with incremental data type validation
- **Incremental Error Handling**: Error recovery mechanisms aware of circuit state and incremental computation semantics
- **Change Stream APIs**: Native support for change-oriented data manipulation optimized for incremental computation patterns

### **IVM Development Console (`web-console/`)**
Interactive web application for visual IVM system development and monitoring:

#### **IVM-Focused Technology Stack**
- **SvelteKit 2.x**: Reactive framework enabling real-time circuit state visualization with Svelte 5 runes for efficient incremental UI updates
- **Circuit Monitoring**: WebSocket integration providing live DBSP circuit execution metrics, operator throughput, and incremental computation performance
- **Secure Circuit Access**: OIDC/OAuth2 authentication protecting access to production IVM deployments

#### **Incremental Computation UX**
- **Visual Circuit Builder**: Interactive SQL-to-DBSP compilation with real-time circuit graph visualization showing operator connections and data flow
- **IVM-Aware SQL Editor**: Syntax highlighting with incremental computation hints, circuit compilation previews, and optimization suggestions
- **Circuit Performance Dashboard**: Real-time visualization of incremental computation metrics including change propagation latency, operator memory usage, and throughput analysis

### **Documentation (`docs.feldera.com/`)**
Comprehensive documentation ecosystem built with Docusaurus:

#### **Content Organization**
- **Getting Started**: Installation and quickstart guides
- **SQL Reference**: Complete SQL function and operator documentation
- **Connectors**: Data source and sink connector documentation
- **Tutorials**: Step-by-step learning materials
- **API Documentation**: Auto-generated from OpenAPI specifications

#### **Interactive Features**
- **Sandbox Integration**: Live SQL examples executable in browser
- **Multi-format Support**: MDX, diagrams, videos, and interactive components

### **Benchmarking (`benchmark/`)**
Comprehensive performance evaluation framework:

#### **Multi-System Comparison**
- **Feldera**: Both Rust-native and SQL implementations
- **Apache Flink**: Standalone and Kafka-integrated configurations
- **Apache Beam**: Multiple runners (Direct, Flink, Spark, Dataflow)

#### **Industry Benchmarks**
- **NEXMark**: 22-query streaming benchmark for auction data
- **TPC-H**: Traditional analytical processing benchmark
- **TikTok**: Custom social media analytics workload

### **CI/CD Infrastructure (`.github/workflows/`)**
Sophisticated automation ecosystem with 16+ specialized workflows:

#### **Hierarchical Architecture**
- **Orchestration Layer**: Main CI coordination and release management
- **Build Foundation**: Multi-platform compilation (Rust, Java, docs, Docker)
- **Testing Validation**: Comprehensive testing across unit, integration, and adapter levels
- **Quality Assurance**: Link validation, failure monitoring, and maintenance
- **Publication**: Automated package publishing to multiple registries

#### **Key Characteristics**
- **Multi-Platform**: Native AMD64 and ARM64 support throughout
- **Performance Optimized**: Extensive caching with S3-compatible storage
- **Containerized**: Consistent build environments with feldera-dev container

## Technical Relationships and Data Flow

### **Development to Production Pipeline**

1. **SQL Development**: Developers write SQL programs using web console or Python SDK
2. **Compilation**: SQL-to-DBSP compiler generates optimized Rust circuits
3. **Runtime Execution**: DBSP engine executes circuits with incremental computation
4. **Data Integration**: Adapters handle input/output with external systems
5. **Monitoring**: Real-time metrics and observability through web console

### **Cross-Component Integration**

#### **Type System Consistency**
- `feldera-types` provides shared type definitions across all components
- OpenAPI specifications ensure client-server consistency
- SQL type system maps to Rust types with null safety guarantees

#### **Configuration Management**
- Unified configuration system across pipeline-manager, adapters, and clients
- Environment-based configuration with validation and defaults
- Secret management and secure credential handling

#### **Error Propagation**
- Structured error types with detailed context information
- Consistent error handling patterns across Rust, Java, and Python components
- User-friendly error messages with actionable guidance

### **DBSP Performance Architecture**

#### **Incremental Computation Model**
- **Change-Oriented Processing**: DBSP circuits process only deltas (insertions, deletions, modifications) rather than full datasets
- **Incremental State Management**: Materialized views and operator state updated incrementally with mathematical guarantees of correctness
- **Bounded Memory Usage**: Memory consumption scales with active state and change rate, not total dataset size, enabling processing of arbitrarily large datasets

#### **Multi-Threaded DBSP Execution**
- **Work-Stealing Scheduler**: DBSP runtime automatically parallelizes circuit execution across available cores with dynamic load balancing
- **Operator Parallelization**: Individual DBSP operators (joins, aggregates) utilize multiple threads with lock-free data structures
- **Pipeline Concurrency**: Multiple independent DBSP circuits execute simultaneously with isolated state management

#### **IVM-Optimized Storage**
- **Incremental Persistence**: Storage backends optimized for change-oriented workloads with efficient delta serialization
- **Circuit State Management**: Persistent storage of DBSP operator state enabling recovery and scaling of long-running incremental computations
- **Build-Time Optimization**: Development workflow acceleration through sccache and incremental compilation of DBSP circuits

## Deployment and Operations

### **Container Strategy**
- Multi-architecture Docker images (AMD64/ARM64) for all components
- Development containers with pre-installed dependencies
- Production containers optimized for size and security

### **Cloud Integration**
- Kubernetes-native deployment with Helm charts
- Multi-cloud support (AWS, GCP, Azure) through cloud-agnostic abstractions
- Auto-scaling based on workload demands

### **Observability**
- Structured logging throughout the platform
- Prometheus metrics integration
- Distributed tracing for complex pipeline debugging
- Health checks and readiness probes

## Development Ecosystem

### **Multi-Language Coordination**
The repository seamlessly integrates four major programming languages, each serving specific purposes while maintaining consistency through shared specifications and automated code generation.

### **Testing Philosophy**
- **Unit Testing**: Component-level testing with comprehensive coverage
- **Integration Testing**: Cross-component testing with real external services
- **Performance Testing**: Continuous benchmarking against industry standards
- **End-to-End Testing**: Complete pipeline testing from SQL to results

### **Quality Assurance**
- **Automated Validation**: Pre-merge validation with fast feedback loops
- **Multi-Platform Testing**: Validation across AMD64 and ARM64 architectures
- **Documentation Testing**: Link validation and content verification
- **Security Scanning**: Dependency and vulnerability scanning

### **Developer Experience**
- **Comprehensive Documentation**: Each component has detailed CLAUDE.md guidance
- **Consistent Tooling**: Standardized build and test commands across components
- **Interactive Development**: Web console for visual pipeline development
- **Performance Monitoring**: Built-in benchmarking and profiling tools

This repository represents a complete platform for incremental stream processing, providing everything needed to develop, deploy, and operate high-performance streaming analytics at scale. The modular architecture enables both ease of development and flexibility in deployment while maintaining the performance characteristics essential for real-time data processing.

## CLAUDE.md Authoring Guide

Follow these governing principles when documenting the repository in CLAUDE.md files.

### Purpose and placement

* Write only what’s needed to work effectively in that directory's context; avoid project history; concise underlying reasoning is OK.

### Layering and scope

* Top level provides a high-level overview: the project’s purpose, key components, core workflows, and dependencies between major areas.
* In the top level, include exactly one short paragraph per current subdirectory describing why it exists and what lives there. Keep it concrete but concise.
* Subdirectory docs add progressively more detail the deeper you go. Each level narrows to responsibilities, interfaces, commands, schemas, and caveats specific to that scope.

### DRY information flow

* Do not repeat what a parent `CLAUDE.md` already states about a subdirectory. Instead, link up to the relevant section.
* Put cross-cutting concepts at the highest level that owns them, and reference from below.
* Keep a single source of truth for contracts, schemas, and commands; everything else links to it.

### Clarity for Claude Code

* Prefer crisp headings, short paragraphs, and tight bullets over prose.
* Name files, entry points, public interfaces, and primary commands explicitly where they belong.
* Call out constraints, feature flags, performance notes, and “gotchas” near the workflows they affect.

### Maintenance rules

* Update the highest applicable level first; ensure lower levels still defer to it.
* Remove stale sections rather than letting them drift; shorter and correct beats exhaustive and outdated.
* When documenting a new directory, add its paragraph to the top level and create its own `CLAUDE.md` that deepens—never duplicates—the parent’s description.

### Quality checklist

* Top level gives a true overview and one concise paragraph per important subdirectory.
* Every subdirectory doc increases detail appropriate to its scope.
* No duplication across levels; links replace repetition.
* Commands, interfaces, and data shapes are precise and current. It is OK to document different arguments for the same command for different use-cases.
* Formatting is skim-friendly and consistent across the repo.
<!-- SECTION:CLAUDE.md END -->

---

## Context: .github/workflows/CLAUDE.md
<!-- SECTION:.github/workflows/CLAUDE.md START -->
## Overview

The `.github/workflows/` directory contains GitHub Actions workflows that form a comprehensive CI/CD ecosystem for the Feldera platform. These workflows orchestrate a sophisticated development pipeline that spans multiple languages, platforms, and deployment targets.

### Workflow Ecosystem Architecture

The Feldera CI/CD pipeline is designed around a **hierarchical workflow architecture** with clear separation of concerns:

#### **Primary Orchestration Layer**
- **`ci.yml`** serves as the main orchestrator for pull requests and main branch changes
- **`ci-pre-mergequeue.yml`** provides fast feedback for pull request validation
- **`ci-release.yml`** coordinates the complete release process
- **`ci-post-release.yml`** handles post-release tasks like package publishing

#### **Build Foundation Layer**
- **`build-rust.yml`** - Multi-platform Rust compilation (AMD64/ARM64)
- **`build-java.yml`** - Java-based SQL compiler and Calcite integration
- **`build-docs.yml`** - Documentation site generation with Docusaurus
- **`build-docker.yml`** - Production Docker images for deployment
- **`build-docker-dev.yml`** - Development environment images for CI

#### **Testing Validation Layer**
- **`test-unit.yml`** - Comprehensive unit testing across all components
- **`test-integration.yml`** - Docker functionality and network isolation testing
- **`test-adapters.yml`** - Multi-architecture I/O adapter validation
- **`test-java.yml`** / **`test-java-nightly.yml`** - SQL compiler testing with extended nightly runs

#### **Quality Assurance Layer**
- **`docs-linkcheck.yml`** - Daily documentation link validation
- **`check-failures.yml`** - Slack notifications for critical workflow failures

#### **Publication Layer**
- **`publish-crates.yml`** - Rust crate publishing to crates.io
- **`publish-python.yml`** - Python SDK publishing to PyPI

### Key Architectural Principles

#### **Multi-Platform First**
- Native AMD64 and ARM64 support across the entire pipeline
- Uses Kubernetes-based runners (`k8s-runners-amd64`, `k8s-runners-arm64`) for scalable execution
- Ensures Feldera runs consistently across different hardware architectures

#### **Containerized Execution**
- Most workflows run in the standardized `feldera-dev` container
- Provides consistent build environments across different runner types
- Eliminates "works on my machine" issues through environment standardization

#### **Performance Optimization**
- **Caching Strategy**: Extensive use of sccache with S3-compatible storage for Rust builds
- **Parallel Execution**: Independent jobs run simultaneously to minimize total pipeline time
- **Artifact Sharing**: Build artifacts are shared between workflows to avoid redundant compilation
- **Documentation-Only Optimization**: Workflows skip expensive CI jobs when only documentation files are changed (see [Documentation-Only Change Optimization](#documentation-only-change-optimization))
- **Resource Selection**: Use appropriate runner types for different workloads

#### **Comprehensive Language Support**
- **Rust**: Core DBSP engine, adapters, and runtime components
- **Java**: SQL-to-DBSP compiler with Apache Calcite integration
- **Python**: SDK and client libraries with comprehensive testing
- **TypeScript/JavaScript**: Documentation site and web console components

#### **Release Automation**
- **Semantic Versioning**: Automated version management across all components
- **Multi-Registry Publishing**: Simultaneous publishing to crates.io, PyPI, and container registries
- **Documentation Deployment**: Automatic documentation updates for each release

#### **Quality Gates**
- **Multi-Stage Validation**: Code must pass unit tests, integration tests, and adapter tests
- **Cross-Platform Testing**: All components validated on both AMD64 and ARM64
- **Documentation Validation**: Links and content verified before release
- **External Service Testing**: Real integration testing with Kafka, PostgreSQL, Redis, and cloud services

### Workflow Interdependencies

The workflows form a **directed acyclic graph (DAG)** of dependencies:

```
ci.yml (orchestrator)
├── build-rust.yml ──────┬──> test-unit.yml
├── build-java.yml ──────┤   test-adapters.yml
├── build-docs.yml       │   test-integration.yml
├── ...
└── Dependencies ────────┴──> build-docker.yml
                              └──> publish-* workflows
```

### Development Lifecycle Integration

#### **Pull Request Flow**
1. **`ci-pre-mergequeue.yml`** provides immediate feedback
2. **`ci.yml`** runs comprehensive validation
3. Quality gates ensure code meets standards before merge

#### **Release Flow**
1. **`ci-release.yml`** creates release from specified commit
2. **`ci-post-release.yml`** publishes packages to registries
3. **`build-docker.yml`** creates versioned container images
4. **`build-docs.yml`** updates documentation site

#### **Maintenance Flow**
1. **`docs-linkcheck.yml`** runs daily to catch documentation issues
2. **`test-java-nightly.yml`** provides extended testing coverage
3. **`check-failures.yml`** monitors critical workflows and alerts team

This ecosystem provides **fast feedback loops** for developers while ensuring **comprehensive validation** and **reliable automation** for the entire Feldera platform development lifecycle.

## Workflows

### `build-docker.yml` - Docker Image Build and Push

**Purpose**: Builds and publishes Docker images for Feldera components to container registries.

**Triggers**:
- Manual workflow dispatch
- Push to main branch (for latest tags)
- Release events (for version tags)

**Key Features**:
- Multi-architecture builds (AMD64, ARM64)
- Registry support for Docker Hub, GitHub Container Registry, and AWS ECR
- Builds multiple Docker images:
  - `pipeline-manager` - Main API service
  - `sql-to-dbsp-compiler` - SQL compilation service
  - Additional utility images
- Uses BuildKit for advanced build features
- Implements layer caching for faster builds
- Tags images with version numbers and latest tags
- Supports both development and release builds

**Runners**: Uses `k8s-runners-amd64` for containerized builds with Docker support.

**Security**:
- Registry authentication via GitHub secrets
- SBOM (Software Bill of Materials) generation
- Image scanning integration

**Outputs**: Published Docker images ready for deployment in various environments.

---

### `ci.yml` - Main Continuous Integration Pipeline

**Purpose**: Orchestrates the complete CI pipeline for pull requests and main branch changes.

**Triggers**:
- Pull request events (opened, synchronized, reopened)
- Push to main branch
- Manual workflow dispatch
- Skipped if only code documentation was changed

**Key Features**:
- **Dependency Matrix**: Builds dependency graph of all CI jobs
- **Parallel Execution**: Runs multiple test suites simultaneously
- **Multi-platform Testing**: Tests across different architectures (AMD64, ARM64)
- **Comprehensive Testing**: Includes unit tests, integration tests, Java tests, and adapter tests
- **Build Validation**: Validates Rust, Java, and documentation builds
- **Artifact Management**: Collects and stores build artifacts for downstream jobs
- **Status Reporting**: Provides consolidated CI status for pull requests

**Workflow Structure**:
- **Build Phase**: Compiles Rust binaries, Java components, and documentation
- **Test Phase**: Executes comprehensive test suite in parallel
- **Integration Phase**: Runs integration tests with real services
- **Reporting Phase**: Aggregates results and reports status

**Runners**: Uses mix of `k8s-runners-amd64` and `k8s-runners-arm64` for comprehensive platform coverage.

**Dependencies**: Coordinates execution of multiple reusable workflows including build-rust, test-unit, test-integration, and others.

---

### `ci-pre-mergequeue.yml` - Pre-Merge Queue Validation

**Purpose**: Provides fast feedback for pull requests with essential validation before merge queue entry.

**Triggers**:
- Pull request events (opened, synchronized)
- Skipped if only code documentation was changed

**Key Features**:
- **Fast Feedback**: Lightweight validation for quick pull request feedback
- **Caching Optimization**: Uses sccache with S3-compatible storage for build caching
- **GitHub App Integration**: Generates tokens for secure repository access
- **Single Job Execution**: Streamlined validation in a single containerized job

**Performance Optimizations**:
- **Build Caching**: RUSTC_WRAPPER with sccache for faster Rust compilation
- **S3 Cache Backend**: Distributed caching across CI runners
- **Container Environment**: Runs in feldera-dev container with pre-installed dependencies

**Security**:
- GitHub App token generation for secure API access
- S3 credentials for cache access via GitHub secrets
- Containerized execution for isolation

**Runners**: Uses `k8s-runners-amd64` with containerized execution environment.

---

### `ci-release.yml` - Release Creation

**Purpose**: Creates new releases by dispatching the release process for specified commits.

**Triggers**:
- Repository dispatch events (trigger-oss-release)
- Manual workflow dispatch with SHA and version inputs

**Key Features**:
- **Release Coordination**: Initiates the complete release process
- **Version Management**: Handles version specification and validation
- **GitHub App Integration**: Uses app tokens for secure repository operations
- **Flexible Triggering**: Supports both automated and manual release triggers

**Input Parameters**:
- `sha_to_release`: Specific commit SHA to create release from
- `version`: Version string for the release

**Security**: Uses GitHub App tokens for repository write permissions and secure release creation.

---

### `build-rust.yml` - Rust Build Workflow

**Purpose**: Compiles Rust components across multiple platforms and architectures.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **Multi-platform Compilation**: Builds for AMD64 and ARM64 architectures
- **Optimized Builds**: Uses release mode with optimizations
- **Artifact Management**: Stores compiled binaries for downstream workflows
- **Caching**: Leverages Rust build caching for faster compilation
- **Cross-compilation**: Handles cross-platform build requirements

**Build Targets**:
- `x86_64-unknown-linux-gnu` (AMD64)
- `aarch64-unknown-linux-gnu` (ARM64)

**Outputs**: Compiled Rust binaries uploaded as GitHub Actions artifacts for use in Docker builds and testing.

---

### `test-integration.yml` - Integration Testing

**Purpose**: Runs comprehensive integration tests including network isolation, Docker functionality, and end-to-end scenarios.

**Triggers**:
- Called by other workflows (workflow_call)
- Manual workflow dispatch with run_id input

**Key Features**:
- **Network Isolation Testing**: Validates pipeline-manager works without network access
- **Docker Integration**: Tests Docker container functionality and health checks
- **Service Validation**: Ensures services start correctly and respond to health checks
- **Multi-Job Testing**: Runs various integration scenarios in parallel

**Test Scenarios**:
- `manager-no-network`: Tests pipeline-manager in isolated network environment
- Container health check validation
- Service startup and readiness verification
- Docker image functionality testing

**Infrastructure**: Uses Docker containers with custom networks for isolation testing.

---

### `build-docs.yml` - Documentation Build

**Purpose**: Builds and validates the Feldera documentation website.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **Docusaurus Build**: Compiles the static documentation site
- **Link Validation**: Ensures all internal links are valid
- **OpenAPI Integration**: Includes API documentation generation
- **Multi-format Support**: Handles MDX, images, and interactive content

**Outputs**: Static documentation site ready for deployment to docs.feldera.com.

---

### `test-unit.yml` - Unit Testing

**Purpose**: Executes comprehensive unit test suites for all Feldera components.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **Comprehensive Coverage**: Tests Rust, Python, and Java components
- **Parallel Execution**: Runs test suites in parallel for faster feedback
- **Test Reporting**: Aggregates and reports test results
- **Failure Analysis**: Provides detailed failure information

**Test Suites**:
- Rust unit tests for core DBSP functionality
- Python SDK tests
- SQL compiler tests
- Integration library tests

---

### `publish-crates.yml` & `publish-python.yml` - Package Publishing

**Purpose**: Publishes Rust crates to crates.io and Python packages to PyPI during releases.

**Triggers**:
- Called by release workflows

**Key Features**:
- **Automated Publishing**: Publishes packages with proper versioning
- **Dependency Management**: Handles cross-package dependencies correctly
- **Registry Authentication**: Securely authenticates with package registries
- **Publication Validation**: Verifies successful package publication

---

### `test-adapters.yml` - Adapter Testing

**Purpose**: Tests I/O adapters across multiple architectures with real external services.

**Triggers**:
- Called by other workflows (workflow_call)
- Manual workflow dispatch with run_id input

**Key Features**:
- **Multi-architecture Testing**: Runs on both AMD64 and ARM64 platforms
- **Service Integration**: Tests with PostgreSQL and other external services
- **Container Environment**: Runs in feldera-dev container with full service stack
- **Matrix Strategy**: Tests adapter compatibility across different architectures

**Test Infrastructure**:
- PostgreSQL 15 service for database testing
- Containerized test execution environment
- Multi-platform validation (x86_64 and aarch64)
- Service health verification and connectivity testing

**Architecture Coverage**: Ensures adapter functionality works consistently across AMD64 and ARM64 platforms.

---

### `docs-linkcheck.yml` - Documentation Link Validation

**Purpose**: Validates all links in the documentation to ensure they remain accessible and correct.

**Triggers**:
- Scheduled runs (daily at 16:00 UTC)
- Manual workflow dispatch

**Key Features**:
- **External Link Checking**: Uses linkchecker to validate docs.feldera.com
- **Selective Validation**: Ignores problematic domains (localhost, crates.io, LinkedIn, etc.)
- **Python Environment**: Uses uv for dependency management
- **No Robot Restrictions**: Bypasses robots.txt restrictions for thorough checking

**Ignored URLs**: Excludes localhost, crates.io, social media sites, and IEEE domains that commonly have access issues.

---

### `test-java.yml` & `test-java-nightly.yml` - Java Testing

**Purpose**: Executes Java-based SQL compiler tests with comprehensive validation.

**Triggers**:
- `test-java.yml`: Called by other workflows (workflow_call)
- `test-java-nightly.yml`: Scheduled nightly runs for extended testing

**Key Features**:
- **SQL Compiler Testing**: Validates SQL-to-DBSP compilation pipeline
- **Extended Test Suite**: Nightly version runs more comprehensive tests
- **Maven Integration**: Uses Maven for Java build and test execution
- **Cross-platform Testing**: Tests Java components across different environments

**Test Coverage**:
- SQL parsing and validation
- DBSP code generation
- Compiler optimization passes
- Integration with Calcite framework

---

### `ci-post-release.yml` - Post-Release Tasks

**Purpose**: Executes post-release tasks after a release is published, including package publishing.

**Triggers**:
- Release events (published)

**Key Features**:
- **Package Publishing**: Coordinates publishing of Python and Rust packages
- **Post-release Automation**: Handles tasks that occur after release creation
- **Build Caching**: Uses sccache for optimized compilation during publishing
- **Parallel Publishing**: Publishes Python packages and Rust crates simultaneously

**Release Tasks**:
- Python package publishing to PyPI
- Rust crate publishing to crates.io
- Post-release validation and verification

---

### `check-failures.yml` - Failure Notification

**Purpose**: Monitors specific workflows and sends Slack notifications when they fail.

**Triggers**:
- Workflow run completion events for monitored workflows
- Specifically watches "Java SLT Nightly" and "Link Checker docs.feldera.com"

**Key Features**:
- **Selective Monitoring**: Only monitors critical workflows on main branch
- **Slack Integration**: Sends formatted notifications to Slack channels
- **Failure Detection**: Triggers on both failure and timeout conditions
- **Rich Notifications**: Includes links to failed workflow runs and repository

**Monitored Conditions**:
- Workflow failure or timeout
- Main branch executions only
- Configurable via CI_DRY_RUN variable

---

### `build-java.yml` - Java Build Workflow

**Purpose**: Compiles Java components including the SQL-to-DBSP compiler.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **SQL Compiler Build**: Compiles the Java-based SQL-to-DBSP compiler
- **Gradle Caching**: Caches Gradle dependencies and Calcite builds
- **Apache Calcite Integration**: Handles Calcite framework dependency
- **Artifact Generation**: Produces JAR files for downstream workflows

**Build Components**:
- SQL-to-DBSP compiler
- Calcite integration components
- Maven-based Java artifacts

---

### `build-docker-dev.yml` - Development Docker Images

**Purpose**: Builds the development Docker image (feldera-dev) used by CI workflows.

**Triggers**:
- Manual workflow dispatch only

**Key Features**:
- **CI Environment**: Creates the base container image used by other workflows
- **Multi-architecture**: Builds for both AMD64 and ARM64 platforms
- **Development Tools**: Includes all tools needed for CI/CD operations
- **Registry Publishing**: Publishes to GitHub Container Registry

**Purpose**: This image contains pre-installed development dependencies and tools used across all other CI workflows, providing a consistent build environment.

---

## Development Workflow Integration

### Local Testing
- Test workflow changes in feature branches before merging
- Use `act` tool for local workflow testing where possible
- Validate Docker builds locally before pushing

### Secrets Management
- All sensitive data stored in GitHub secrets
- Environment-specific secrets for different deployment targets
- Regular rotation of authentication tokens

### Monitoring and Debugging
- Workflow run logs available in GitHub Actions tab
- Failed builds trigger notifications
- Performance metrics tracked for build times and success rates

## Documentation-Only Change Optimization

The Feldera CI/CD pipeline has been optimized to skip expensive CI jobs when only code documentation files (`CLAUDE.md` and `README.md`) are changed. This optimization significantly reduces resource consumption and provides faster feedback for code documentation updates.

### Implementation Strategy

- **`ci-pre-mergequeue.yml`** uses GitHub's native `paths-ignore` filtering:
- **`ci.yml`** uses custom conditional execution since GitHub Actions doesn't support `paths-ignore` for `merge_group` events

## Best Practices

### Workflow Development
- **Modular Design**: Break complex workflows into reusable actions
- **Conditional Logic**: Use appropriate conditions to skip unnecessary steps
- **Resource Efficiency**: Optimize for faster execution times
- **Error Handling**: Include proper error handling and cleanup

### Security Considerations
- **Least Privilege**: Use minimal required permissions
- **Secret Protection**: Never log or expose secrets in output
- **Dependency Scanning**: Regularly update action dependencies
- **Supply Chain Security**: Pin action versions to specific SHAs

### [Performance Optimization](#performance-optimization)

This documentation will be expanded with additional workflow descriptions as they are analyzed and documented.
<!-- SECTION:.github/workflows/CLAUDE.md END -->

---

## Context: benchmark/CLAUDE.md
<!-- SECTION:benchmark/CLAUDE.md START -->
## Overview

The `benchmark/` directory contains comprehensive benchmarking infrastructure for comparing Feldera's performance against other stream processing systems. It implements industry-standard benchmarks (NEXMark, TPC-H, TikTok) across multiple platforms to provide objective performance comparisons.

## Benchmark Ecosystem

### Supported Systems

The benchmarking framework supports comparative analysis across:

- **Feldera** - Both Rust-native and SQL implementations
- **Apache Flink** - Standalone and Kafka-integrated configurations
- **Apache Beam** - Multiple runners:
  - Direct runner (development/testing)
  - Flink runner
  - Spark runner
  - Google Cloud Dataflow runner

### Benchmark Suites

#### **NEXMark Benchmark**
- **Industry Standard**: Streaming benchmark for auction data processing
- **22 Queries**: Complete suite of streaming analytics queries (q0-q22)
- **Realistic Data**: Auction, bidder, and seller event generation
- **Multiple Modes**: Streaming and batch processing modes

#### **TPC-H Benchmark**
- **OLAP Standard**: Traditional analytical processing benchmark
- **22 Queries**: Complex analytical queries for business intelligence
- **Batch Processing**: Focus on analytical query performance

#### **TikTok Benchmark**
- **Custom Workload**: Social media analytics patterns
- **Streaming Focus**: Real-time social media data processing

## Key Development Commands

### Running Individual Benchmarks

```bash
# Basic Feldera benchmark
./run-nexmark.sh --runner=feldera --events=100M

# Compare Feldera vs Flink
./run-nexmark.sh --runner=flink --events=100M

# SQL implementation on Feldera
./run-nexmark.sh --runner=feldera --language=sql

# Batch processing mode
./run-nexmark.sh --batch --events=100M

# Specific query testing
./run-nexmark.sh --query=q3 --runner=feldera

# Core count specification
./run-nexmark.sh --cores=8 --runner=feldera
```

### Running Benchmark Suites

```bash
# Full benchmark suite using Makefile
make -f suite.mk

# Limited runners and modes
make -f suite.mk runners='feldera flink' modes=batch events=1M

# Specific configuration
make -f suite.mk runners=feldera events=100M cores=16
```

### Analysis and Results

```bash
# Generate analysis (requires PSPP/SPSS)
pspp analysis.sps

# View results in CSV format
cat nexmark.csv
```

## Project Structure

### Core Scripts
- `run-nexmark.sh` - Main benchmark execution script
- `suite.mk` - Makefile for running comprehensive benchmark suites
- `analysis.sps` - SPSS/PSPP script for statistical analysis

### Implementation Directories

#### `feldera-sql/`
- **SQL Benchmarks**: Pure SQL implementations of benchmark queries
- **Pipeline Management**: Integration with Feldera's pipeline manager
- **Query Definitions**: Standard benchmark queries in SQL format
- **Table Schemas**: Database schema definitions for benchmarks

#### `flink/` & `flink-kafka/`
- **Flink Integration**: Standalone and Kafka-integrated Flink setups
- **Docker Containers**: Containerized Flink environments
- **Configuration**: Flink-specific performance tuning configurations
- **NEXMark Implementation**: Java-based NEXMark implementation

#### `beam/`
- **Apache Beam**: Multi-runner Beam implementations
- **Language Support**: Java, SQL (Calcite), and ZetaSQL implementations
- **Cloud Integration**: Google Cloud Dataflow configuration
- **Setup Scripts**: Environment preparation and dependency management

## Important Implementation Details

### Performance Optimization

#### **Feldera Optimizations**
- **Storage Configuration**: Uses `/tmp` by default, configure `TMPDIR` for real filesystem
- **Multi-threading**: Automatic core detection with 16-core maximum default
- **Memory Management**: Efficient incremental computation with minimal memory overhead

#### **System-Specific Tuning**
- **Flink**: RocksDB and HashMap state backends available
- **Beam**: Multiple language implementations (Java, SQL, ZetaSQL)
- **Cloud**: Optimized configurations for cloud deployments

### Benchmark Modes

#### **Streaming Mode (Default)**
- **Real-time Processing**: Continuous data processing simulation
- **Incremental Results**: Measure throughput and latency
- **Event Generation**: Configurable event rates and patterns

#### **Batch Mode**
- **Analytical Processing**: Traditional batch analytics
- **Complete Data**: Process entire datasets at once
- **Throughput Focus**: Optimized for maximum data processing rates

### Data Generation

- **Configurable Scale**: From 100K to 100M+ events
- **Realistic Patterns**: Auction data with realistic distributions
- **Reproducible**: Deterministic data generation for consistent comparisons

## Development Workflow

### For New Benchmarks

1. Add query definitions to appropriate `benchmarks/*/queries/` directory
2. Update table schemas in `table.sql` files
3. Implement runner-specific logic in system directories
4. Add query to `run-nexmark.sh` query list
5. Test across multiple systems for consistency

### For System Integration

1. Create system-specific directory (e.g., `newsystem/`)
2. Implement setup and configuration scripts
3. Add runner option to `run-nexmark.sh`
4. Update `suite.mk` runner list
5. Document setup requirements

### Testing Strategy

#### **Correctness Validation**
- **Cross-System Consistency**: Results should match across systems
- **Query Verification**: Validate SQL semantics and outputs
- **Edge Case Testing**: Test with various data sizes and patterns

#### **Performance Analysis**
- **Throughput Measurement**: Events processed per second
- **Latency Analysis**: End-to-end processing delays
- **Resource Usage**: CPU, memory, and I/O utilization
- **Scalability Testing**: Performance across different core counts

### Configuration Management

#### **Environment Variables**
- `TMPDIR` - Storage location for temporary files
- `FELDERA_API_URL` - Pipeline manager endpoint (default: localhost:8080)
- Cloud credentials for Dataflow benchmarks

#### **System Requirements**
- **Java 11+** - Required for Beam and Flink
- **Docker** - For containerized system testing
- **Python 3** - For analysis scripts
- **Cloud SDK** - For Google Cloud Dataflow testing

### Results Analysis

#### **Statistical Analysis**
- **PSPP Integration**: Statistical analysis with `analysis.sps`
- **Performance Tables**: Formatted comparison tables
- **Trend Analysis**: Performance trends across system configurations

#### **Output Formats**
- **CSV Results**: Machine-readable performance data
- **Formatted Tables**: Human-readable comparison tables
- **Statistical Reports**: Detailed statistical analysis

## Best Practices

### Benchmark Execution
- **Warm-up Runs**: Allow systems to reach steady state
- **Multiple Iterations**: Run benchmarks multiple times for statistical significance
- **Resource Isolation**: Ensure consistent resource availability
- **Environment Control**: Use consistent hardware and software configurations

### Performance Comparison
- **Fair Comparison**: Use equivalent configurations across systems
- **Resource Limits**: Apply consistent memory and CPU limits
- **Data Consistency**: Use identical datasets across systems
- **Metric Standardization**: Use consistent performance metrics

### System Setup
- **Documentation**: Follow setup instructions for each system
- **Version Control**: Pin specific versions for reproducible results
- **Configuration**: Use optimized configurations for each system
- **Monitoring**: Monitor resource usage during benchmarks

This benchmarking infrastructure provides comprehensive tools for validating Feldera's performance advantages and identifying optimization opportunities across different workloads and system configurations.
<!-- SECTION:benchmark/CLAUDE.md END -->

---

## Context: crates/adapterlib/CLAUDE.md
<!-- SECTION:crates/adapterlib/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the adapterlib crate
cargo build -p adapterlib

# Run tests
cargo test -p adapterlib

# Check documentation
cargo doc -p adapterlib --open
```

## Architecture Overview

### Technology Stack

- **Transport Abstraction**: Generic transport layer interface
- **Format Processing**: Data format abstraction and utilities
- **Catalog System**: Runtime circuit introspection
- **Error Handling**: Comprehensive error types for I/O operations

### Core Purpose

Adapter Library provides **foundational abstractions** for building I/O adapters:

- **Transport Traits**: Generic interfaces for input/output transports
- **Format Abstractions**: Data serialization/deserialization framework
- **Catalog Interface**: Runtime circuit inspection and control
- **Utility Functions**: Common functionality for adapter implementations

### Project Structure

#### Core Modules

- `src/transport.rs` - Transport layer abstractions and traits
- `src/format.rs` - Data format processing abstractions
- `src/catalog.rs` - Circuit catalog and introspection
- `src/errors/` - Error types for adapter operations
- `src/utils/` - Utility functions and helpers

## Important Implementation Details

### Transport Abstraction

#### Core Transport Traits
```rust
pub trait InputTransport {
    type Error;

    async fn start(&mut self) -> Result<(), Self::Error>;
    async fn read_batch(&mut self) -> Result<Vec<ParsedEvent>, Self::Error>;
    async fn stop(&mut self) -> Result<(), Self::Error>;
}

pub trait OutputTransport {
    type Error;

    async fn start(&mut self) -> Result<(), Self::Error>;
    async fn write_batch(&mut self, data: &[OutputEvent]) -> Result<(), Self::Error>;
    async fn stop(&mut self) -> Result<(), Self::Error>;
}
```

#### Transport Features
- **Async Interface**: Non-blocking I/O operations
- **Batch Processing**: Efficient bulk data transfer
- **Error Handling**: Transport-specific error types
- **Lifecycle Management**: Start/stop semantics

### Format Processing

#### Format Traits
```rust
pub trait Deserializer {
    type Error;
    type Output;

    fn deserialize(&mut self, data: &[u8]) -> Result<Self::Output, Self::Error>;
}

pub trait Serializer {
    type Error;
    type Input;

    fn serialize(&mut self, data: &Self::Input) -> Result<Vec<u8>, Self::Error>;
}
```

#### Format Features
- **Type Safety**: Strongly typed serialization
- **Error Recovery**: Graceful handling of malformed data
- **Schema Evolution**: Handle schema changes
- **Performance**: Zero-copy where possible

### Catalog System

#### Circuit Introspection
```rust
pub trait Catalog {
    fn input_handles(&self) -> Vec<InputHandle>;
    fn output_handles(&self) -> Vec<OutputHandle>;
    fn get_input_handle(&self, name: &str) -> Option<InputHandle>;
    fn get_output_handle(&self, name: &str) -> Option<OutputHandle>;
}
```

#### Runtime Control
- **Handle Discovery**: Find inputs and outputs by name
- **Schema Inspection**: Query table and view schemas
- **Type Information**: Runtime type metadata
- **Statistics**: Performance and throughput metrics

### Error Handling Framework

#### Error Categories
```rust
pub enum AdapterError {
    TransportError(TransportError),
    FormatError(FormatError),
    ConfigurationError(String),
    RuntimeError(String),
}
```

#### Error Features
- **Structured Errors**: Rich error information with context
- **Error Chaining**: Preserve error causation chains
- **Recovery Hints**: Actionable error messages
- **Logging Integration**: Structured logging support

### Utility Functions

#### Common Patterns
- **Retry Logic**: Configurable retry mechanisms
- **Rate Limiting**: Throughput control utilities
- **Buffer Management**: Efficient buffer handling
- **Connection Pooling**: Resource management helpers

#### Data Processing Utilities
- **Type Conversion**: Between different data representations
- **Validation**: Data integrity checking
- **Transformation**: Common data transformations
- **Batching**: Batch size optimization

## Development Workflow

### For Transport Implementation

1. Implement `InputTransport` or `OutputTransport` trait
2. Define transport-specific error types
3. Add configuration types for transport parameters
4. Implement lifecycle methods (start/stop)
5. Add comprehensive error handling
6. Test with various failure scenarios

### For Format Support

1. Implement `Deserializer` or `Serializer` trait
2. Handle schema validation and evolution
3. Add performance optimizations
4. Test with malformed data
5. Document format-specific behavior
6. Add benchmarks for performance validation

### Testing Strategy

#### Unit Tests
- **Interface Compliance**: Trait implementation correctness
- **Error Handling**: Comprehensive error scenario testing
- **Edge Cases**: Boundary conditions and limits
- **Resource Management**: Proper cleanup and lifecycle

#### Integration Tests
- **End-to-End**: Complete adapter pipeline testing
- **Performance**: Throughput and latency validation
- **Reliability**: Failure recovery and resilience
- **Compatibility**: Cross-format and cross-transport testing

### Design Patterns

#### Async Patterns
```rust
// Async transport implementation
impl InputTransport for MyTransport {
    async fn read_batch(&mut self) -> Result<Vec<ParsedEvent>, Self::Error> {
        // Non-blocking batch read
        let data = self.connection.read_batch().await?;
        Ok(self.format.deserialize_batch(&data)?)
    }
}
```

#### Error Handling Patterns
```rust
// Comprehensive error handling
match transport.read_batch().await {
    Ok(events) => process_events(events),
    Err(TransportError::ConnectionLost) => reconnect_and_retry(),
    Err(TransportError::Timeout) => continue_with_empty_batch(),
    Err(e) => return Err(e.into()),
}
```

### Performance Considerations

#### Batch Processing
- **Optimal Batch Size**: Balance latency vs throughput
- **Memory Usage**: Avoid excessive buffering
- **CPU Utilization**: Efficient serialization/deserialization
- **Network I/O**: Minimize network round trips

#### Resource Management
- **Connection Pooling**: Reuse expensive resources
- **Buffer Reuse**: Minimize allocations
- **Async Efficiency**: Non-blocking operations
- **Cleanup**: Proper resource disposal

### Configuration Files

- `Cargo.toml` - Core dependencies for transport and format abstractions
- Minimal dependencies to avoid bloat
- Feature flags for optional functionality

### Dependencies

#### Core Dependencies
- `tokio` - Async runtime
- `serde` - Serialization framework
- `anyhow` - Error handling
- `tracing` - Structured logging

#### Optional Dependencies
- `datafusion` - SQL query engine integration
- Various format-specific libraries

### Best Practices

#### API Design
- **Trait-Based**: Use traits for extensibility
- **Async First**: Design for async from the start
- **Error Rich**: Provide detailed error information
- **Performance Aware**: Consider performance implications

#### Implementation Patterns
- **Resource Safety**: Proper RAII patterns
- **Error Propagation**: Use `?` operator consistently
- **Logging**: Comprehensive structured logging
- **Testing**: Test both success and failure paths

#### Documentation
- **Trait Documentation**: Clear trait contract specification
- **Examples**: Comprehensive usage examples
- **Error Scenarios**: Document error conditions
- **Performance Notes**: Performance characteristics

This crate provides the foundational abstractions that make it easy to implement new I/O adapters while maintaining consistency and performance across the Feldera platform.
<!-- SECTION:crates/adapterlib/CLAUDE.md END -->

---

## Context: crates/adapters/CLAUDE.md
<!-- SECTION:crates/adapters/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the adapters crate
cargo build -p adapters

# Run tests
cargo test -p adapters

# Run tests with required dependencies (Kafka, Redis, etc.)
cargo test -p adapters --features=with-kafka

# Build with all features
cargo build -p adapters --all-features
```

### Running Examples

```bash
# Run server demo with Kafka support
cargo run --example server --features="with-kafka server"

# Test specific transport integrations
cargo test -p adapters test_kafka
cargo test -p adapters test_http
```

### Development Environment

```bash
# Start required services for testing
docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v24.2.4 redpanda start --smp 2

# Install system dependencies
sudo apt install cmake
```

## Architecture Overview

### Technology Stack

- **I/O Framework**: Async I/O with tokio runtime
- **Serialization**: Multiple format support (JSON, CSV, Avro, Parquet)
- **Transport Layer**: Kafka, HTTP, File, Redis, S3 integrations
- **Database Integration**: PostgreSQL and Delta Lake connectors
- **Testing**: Comprehensive integration testing with external services

### Core Concepts

The Adapters crate provides a **unified I/O framework** for DBSP circuits:

- **Input Adapters**: Ingest data from external sources into DBSP circuits
- **Output Adapters**: Stream circuit outputs to external consumers
- **Transport Layer**: Pluggable transport mechanisms
- **Format Layer**: Serialization/deserialization for different data formats
- **Controller**: Orchestrates circuit execution with I/O adapters

### Project Structure

#### Core Directories

- `src/adhoc/` - Implementation for the ad-hoc queries that uses Apache DataFusion batch query engine
- `src/format/` - Data format handlers (JSON, CSV, Avro, Parquet)
- `src/transport/` - Transport implementations (Kafka, HTTP, File, etc.)
- `src/integrated/` - Integrated connectors (Postgres, Delta Lake)
- `src/controller/` - Circuit controller and lifecycle management
- `src/static_compile/` - Compile-time adapter generation
- `src/test/` - Testing utilities and mock implementations

#### Key Components

- **Transport Adapters**: Protocol-specific I/O implementations
- **Format Processors**: Data serialization/deserialization
- **Controller**: Circuit execution coordinator
- **Catalog**: Runtime circuit introspection and control

## Important Implementation Details

### Transport Layer

#### Supported Transports

**Kafka Integration**
```rust
// Fault-tolerant Kafka producer
use adapters::transport::kafka::ft::KafkaOutputTransport;

// Non-fault-tolerant (higher performance)
use adapters::transport::kafka::nonft::KafkaOutputTransport;
```

**HTTP REST API**
```rust
use adapters::transport::http::{HttpInputTransport, HttpOutputTransport};
```

**File I/O**
```rust
use adapters::transport::file::{FileInputTransport, FileOutputTransport};
```

**Redis Streams**
```rust
use adapters::transport::redis::RedisOutputTransport;
```

### Format Layer

#### Supported Formats

- **CSV**: Delimited text with schema inference
- **JSON**: Nested data structures with type coercion
- **Avro**: Schema-based binary serialization
- **Parquet**: Columnar data format
- **Raw**: Unprocessed byte streams

#### Format Pipeline

1. **Deserialization**: Convert external format to internal representation
2. **Type Coercion**: Map external types to DBSP types
3. **Validation**: Ensure data integrity and constraints
4. **Batch Processing**: Optimize throughput with batching

### Controller Architecture

```rust
use adapters::controller::Controller;

// Create controller with circuit
let controller = Controller::new(circuit, adapters)?;

// Start data processing
controller.start().await?;

// Monitor and control
controller.step().await?;
controller.pause().await?;
```

#### Controller Features

- **Lifecycle Management**: Start, pause, stop, checkpoint
- **Error Handling**: Graceful degradation and recovery
- **Statistics**: Performance monitoring and metrics
- **Flow Control**: Backpressure and rate limiting

### Integrated Connectors

#### PostgreSQL CDC
```rust
use adapters::integrated::postgres::PostgresInputAdapter;

// Real-time change data capture
let adapter = PostgresInputAdapter::new(config).await?;
```

#### Delta Lake
```rust
use adapters::integrated::delta_table::DeltaTableOutputAdapter;

// Write to Delta Lake format
let adapter = DeltaTableOutputAdapter::new(config).await?;
```

## Development Workflow

### For New Transport Implementation

1. Create module in `src/transport/`
2. Implement `InputTransport` and/or `OutputTransport` traits
3. Add configuration types in `feldera-types`
4. Implement error handling and reconnection logic
5. Add comprehensive tests with real service
6. Update feature flags in `Cargo.toml`

### For New Format Support

1. Add format module in `src/format/`
2. Implement `Deserializer` and/or `Serializer` traits
3. Add schema inference and validation
4. Handle type coercion edge cases
5. Add performance benchmarks
6. Test with various data patterns

### Testing Strategy

#### Unit Tests
- Mock implementations for isolated testing
- Format conversion correctness
- Error handling scenarios

#### Integration Tests
- Real external service dependencies
- End-to-end data flow validation
- Performance and reliability testing

#### Test Dependencies

The test suite requires external services:

- **Kafka/Redpanda**: Message queue testing
- **PostgreSQL**: Database connector testing
- **Redis**: Stream testing
- **S3-compatible**: Object storage testing

### Performance Optimization

#### Throughput Optimization
- **Batch Processing**: Amortize per-record overhead
- **Connection Pooling**: Reuse network connections
- **Parallel Processing**: Multi-threaded I/O operations
- **Zero-Copy**: Minimize memory allocations

#### Memory Management
- **Streaming Processing**: Bounded memory usage
- **Backpressure**: Flow control mechanisms
- **Buffer Management**: Optimal buffer sizing
- **Garbage Collection**: Efficient cleanup

### Configuration Files

- `Cargo.toml` - Feature flags and dependencies
- `lsan.supp` - Leak sanitizer suppressions
- Test data files in various formats

### Key Features

- **Fault Tolerance**: Automatic reconnection and retry logic
- **Schema Evolution**: Handle schema changes gracefully
- **Monitoring**: Built-in metrics and observability
- **Multi-format**: Support for diverse data formats

### Dependencies

#### Core Dependencies
- `tokio` - Async runtime
- `serde` - Serialization framework
- `anyhow` - Error handling
- `tracing` - Structured logging

#### Transport Dependencies
- `rdkafka` - Kafka client
- `reqwest` - HTTP client
- `redis` - Redis client
- `rusoto_s3` - AWS S3 client

#### Format Dependencies
- `csv` - CSV processing
- `serde_json` - JSON processing
- `apache-avro` - Avro format support
- `parquet` - Columnar data format

### Error Handling Patterns

- **Transient Errors**: Retry with exponential backoff
- **Permanent Errors**: Fail fast with detailed diagnostics
- **Schema Errors**: Graceful degradation with data preservation
- **Network Errors**: Connection management and recovery

### Security Considerations

- **Authentication**: Support for various auth mechanisms
- **Encryption**: TLS/SSL for network transports
- **Access Control**: Fine-grained permissions
- **Data Privacy**: PII handling and sanitization

### Monitoring and Observability

- **Metrics**: Throughput, latency, error rates
- **Tracing**: Distributed request tracing
- **Logging**: Structured log output
- **Health Checks**: Service health monitoring

### Best Practices

- **Async Design**: Non-blocking I/O operations
- **Resource Management**: Proper cleanup and lifecycle
- **Error Propagation**: Structured error handling
- **Testing**: Real service integration tests
- **Documentation**: Comprehensive examples and guides
<!-- SECTION:crates/adapters/CLAUDE.md END -->

---

## Context: crates/adapters/src/adhoc/CLAUDE.md
<!-- SECTION:crates/adapters/src/adhoc/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the adhoc module as part of adapters crate
cargo build -p adapters

# Run tests for the adhoc module
cargo test -p adapters adhoc

# Run tests with DataFusion features enabled
cargo test -p adapters --features="with-datafusion"

# Check documentation
cargo doc -p adapters --open
```

### Running Ad-hoc Queries

```bash
# Via CLI
fda exec pipeline-name "SELECT * FROM materialized_view"

# Interactive shell
fda shell pipeline-name

# Via HTTP API
curl "http://localhost:8080/v0/pipelines/my-pipeline/query?sql=SELECT%20*%20FROM%20users&format=json"
```

## Architecture Overview

### Technology Stack

- **Query Engine**: Apache DataFusion for SQL query execution
- **Output Formats**: Text tables, JSON, Parquet, Arrow IPC
- **Transport**: WebSocket and HTTP streaming
- **Runtime**: Multi-threaded execution using DBSP's Tokio runtime
- **Storage**: Direct access to DBSP's materialized state

### Core Purpose

The Ad-hoc Query module provides **batch SQL query capabilities** alongside Feldera's core incremental processing:

- **Interactive Queries**: Real-time SQL queries against current pipeline state
- **Multiple Formats**: Support for various output formats optimized for different use cases
- **Streaming Results**: Memory-efficient streaming of large result sets
- **DataFusion Integration**: Full SQL compatibility through Apache DataFusion engine

This is the primary module that is responsible for ad-hoc queries backend functionality.

### Project Structure

#### Core Files

- `mod.rs` - Main module with WebSocket handling and session context creation
- `executor.rs` - Query execution and result streaming implementations
- `format.rs` - Text table formatting utilities
- `table.rs` - DataFusion table provider and INSERT operation support

## Implementation Details

### DataFusion Session Configuration

#### Session Context Setup
```rust
// From mod.rs - actual DataFusion configuration
pub(crate) fn create_session_context(config: &PipelineConfig) -> Result<SessionContext> {
    const SORT_IN_PLACE_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
    const SORT_SPILL_RESERVATION_BYTES: usize = 64 * 1024 * 1024;

    let session_config = SessionConfig::new()
        .with_target_partitions(config.global.workers as usize)
        .with_sort_in_place_threshold_bytes(SORT_IN_PLACE_THRESHOLD_BYTES)
        .with_sort_spill_reservation_bytes(SORT_SPILL_RESERVATION_BYTES);

    // Memory pool configuration for large queries
    let mut runtime_env_builder = RuntimeEnvBuilder::new();
    if let Some(memory_mb_max) = config.global.resources.memory_mb_max {
        let memory_bytes_max = memory_mb_max * 1024 * 1024;
        runtime_env_builder = runtime_env_builder
            .with_memory_pool(Arc::new(FairSpillPool::new(memory_bytes_max as usize)));
    }

    // Spill-to-disk directory for temp files during large operations
    if let Some(storage) = &config.storage_config {
        let path = PathBuf::from(storage.path.clone()).join("adhoc-tmp");
        runtime_env_builder = runtime_env_builder.with_temp_file_path(path);
    }
}
```

### Query Execution Pipeline

#### Multi-Threaded Execution Strategy
```rust
// From executor.rs - execution in DBSP runtime for parallelization
fn execute_stream(df: DataFrame) -> Receiver<DFResult<SendableRecordBatchStream>> {
    let (tx, rx) = oneshot::channel();
    // Execute in DBSP's multi-threaded runtime, not actix-web's single-threaded runtime
    dbsp::circuit::tokio::TOKIO.spawn(async move {
        let _r = tx.send(df.execute_stream().await);
    });
    rx
}
```

#### Result Streaming Implementation
```rust
// Four different streaming formats implemented:

// 1. Text format - human-readable tables
pub(crate) fn stream_text_query(df: DataFrame)
    -> impl Stream<Item = Result<ByteString, PipelineError>>

// 2. JSON format - line-delimited JSON records (deprecated)
pub(crate) fn stream_json_query(df: DataFrame)
    -> impl Stream<Item = Result<ByteString, PipelineError>>

// 3. Arrow IPC format - high-performance binary streaming
pub(crate) fn stream_arrow_query(df: DataFrame)
    -> impl Stream<Item = Result<Bytes, DataFusionError>>

// 4. Parquet format - columnar data with compression
pub(crate) fn stream_parquet_query(df: DataFrame)
    -> impl Stream<Item = Result<Bytes, DataFusionError>>
```

### WebSocket Handler Implementation

#### Real-time Query Processing
```rust
// From mod.rs - WebSocket message handling
pub async fn adhoc_websocket(
    df_session: SessionContext,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, PipelineError> {
    let (res, mut ws_session, stream) = actix_ws::handle(&req, stream)?;
    let mut stream = stream
        .max_frame_size(MAX_WS_FRAME_SIZE)  // 2MB frame limit
        .aggregate_continuations()
        .max_continuation_size(4 * MAX_WS_FRAME_SIZE);

    // Process WebSocket messages
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(AggregatedMessage::Text(text)) => {
                let args = serde_json::from_str::<AdhocQueryArgs>(&text)?;
                let df = df_session
                    .sql_with_options(&args.sql, SQLOptions::new().with_allow_ddl(false))
                    .await?;
                adhoc_query_handler(df, ws_session.clone(), args).await?;
            }
            // Handle other message types...
        }
    }
}
```

### Table Provider Implementation

#### DBSP State Integration
```rust
// From table.rs - AdHocTable provides DataFusion access to DBSP materialized state
pub struct AdHocTable {
    controller: Weak<ControllerInner>,           // Weak ref to avoid cycles
    input_handle: Option<Box<dyn DeCollectionHandle>>,  // For INSERT operations
    name: SqlIdentifier,
    materialized: bool,                          // Only materialized tables are queryable
    schema: Arc<Schema>,
    snapshots: ConsistentSnapshots,              // Current state snapshots
}

#[async_trait]
impl TableProvider for AdHocTable {
    async fn scan(&self, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>)
        -> Result<Arc<dyn ExecutionPlan>> {

        // Validate materialization requirement
        if !self.materialized {
            return Err(DataFusionError::Execution(
                format!("Make sure `{}` is configured as materialized: \
                use `with ('materialized' = 'true')` for tables, or `create materialized view` for views",
                self.name)
            ));
        }

        // Create execution plan
        Ok(Arc::new(AdHocQueryExecution::new(/* ... */)))
    }
}
```

#### INSERT Operation Support
```rust
// From table.rs - INSERT INTO materialized tables
async fn insert_into(&self, input: Arc<dyn ExecutionPlan>, overwrite: InsertOp)
    -> Result<Arc<dyn ExecutionPlan>> {

    match &self.input_handle {
        Some(ih) => {
            // Create temporary adhoc input endpoint
            let endpoint_name = format!("adhoc-ingress-{}-{}", self.name.name(), Uuid::new_v4());
            let sink = Arc::new(AdHocTableSink::new(/* ... */));
            Ok(Arc::new(DataSinkExec::new(input, sink, None)))
        }
        None => exec_err!("Called insert_into on a view")
    }
}
```

### Query Execution Details

#### Record Batch Processing
```rust
// From table.rs - efficient batch processing from DBSP cursors
let mut cursor = batch_reader.cursor(RecordFormat::Parquet(
    output_adhoc_arrow_serde_config().clone()
))?;

const MAX_BATCH_SIZE: usize = 256;  // Optimized for latency vs throughput
let mut cur_batch_size = 0;

while cursor.key_valid() {
    if cursor.weight() > 0 {  // Skip deleted records
        cursor.serialize_key_to_arrow(&mut insert_builder.builder)?;
        cur_batch_size += 1;

        if cur_batch_size >= MAX_BATCH_SIZE {
            let batch = insert_builder.builder.to_record_batch()?;
            send_batch(&tx, &projection, batch).await?;
            cur_batch_size = 0;
        }
    }
    cursor.step_key();
}
```

### Output Format Implementations

#### Text Table Formatting
```rust
// From format.rs - human-readable table output
pub(crate) fn create_table(results: &[RecordBatch]) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");  // comfy-table preset

    // Add headers from schema
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }

    // Process each row with cell length limiting
    const CELL_MAX_LENGTH: usize = 64;
    for formatter in &formatters {
        let mut content = formatter.value(row).to_string();
        if content.len() > CELL_MAX_LENGTH {
            content.truncate(CELL_MAX_LENGTH);
            content.push_str("...");
        }
    }
}
```

#### Parquet Streaming
```rust
// From executor.rs - efficient Parquet streaming with compression
const PARQUET_CHUNK_SIZE: usize = MAX_WS_FRAME_SIZE / 2;  // 1MB chunks

let mut writer = AsyncArrowWriter::try_new(
    ChannelWriter::new(tx),
    schema,
    Some(WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()),
)?;

// Stream with controlled chunk sizes
while let Some(batch) = stream.next().await.transpose()? {
    writer.write(&batch).await?;
    if writer.in_progress_size() > PARQUET_CHUNK_SIZE {
        writer.flush().await?;  // Send chunk to client
    }
}
```

## Key Features and Limitations

### Supported Operations
- **SELECT queries** from materialized tables and views
- **Complex queries** with joins, aggregations, window functions
- **INSERT statements** into materialized tables
- **Multiple output formats** for different use cases
- **Streaming results** for memory efficiency

### Critical Limitations
- **Materialization Required**: Only `WITH ('materialized' = 'true')` tables/views are queryable
- **No DDL Operations**: CREATE, ALTER, DROP are explicitly disabled
- **SQL Dialect Differences**: DataFusion SQL vs Feldera SQL (Calcite-based) differences
- **Resource Sharing**: Shares CPU/memory with main pipeline processing
- **WebSocket Frame Limits**: 2MB maximum frame size

### Configuration Requirements

#### Pipeline Configuration
```sql
-- Tables must be explicitly materialized
CREATE TABLE users (id INT, name STRING) WITH ('materialized' = 'true');

-- Or use materialized views
CREATE MATERIALIZED VIEW user_stats AS
  SELECT COUNT(*) as count, AVG(age) as avg_age FROM users;
```

#### Memory and Threading
- Uses pipeline's configured worker thread count
- Memory limits configurable via pipeline resources
- Spill-to-disk support for large operations
- Automatic parallelization across available cores

## Development Workflow

### Adding New Output Formats

1. Add format enum variant to `AdHocResultFormat` in feldera-types
2. Implement streaming function in `executor.rs`
3. Add format handling in `adhoc_query_handler()` in `mod.rs`
4. Add HTTP response handling in `stream_adhoc_result()`
5. Update client SDKs and documentation

### Performance Optimization

#### Memory Management
- Configure appropriate memory pools for large queries
- Use spill-to-disk for operations exceeding memory limits
- Optimize batch sizes for latency vs throughput trade-offs
- Monitor WebSocket frame sizes to prevent timeouts

#### Query Performance
- Leverage DataFusion's query optimizer
- Use projection pushdown when possible
- Consider materialized view design for common queries
- Monitor resource usage impact on main pipeline

### Error Handling Patterns

#### Materialization Validation
```rust
if !self.materialized {
    return Err(DataFusionError::Execution(
        format!("Tried to SELECT from a non-materialized source. Make sure `{}` is configured as materialized",
                self.name)
    ));
}
```

#### Resource Management Errors
- Memory limit exceeded → automatic spill to disk
- WebSocket connection lost → graceful cleanup
- Query timeout → configurable limits with clear messages
- Invalid SQL → DataFusion parser error propagation

## Dependencies and Integration

### Core Dependencies
- **datafusion** - SQL query engine and execution
- **arrow** - Columnar data processing and formats
- **actix-web** - WebSocket and HTTP handling
- **tokio** - Async runtime and streaming
- **parquet** - Columnar file format support

### Integration Points
- **Controller**: Access to circuit state and snapshots
- **Transport**: AdHoc input endpoints for INSERT operations
- **Storage**: Direct access to materialized table storage
- **Types**: Shared configuration and error types

### Performance Characteristics
- **Throughput**: Optimized for interactive query latency
- **Memory Usage**: Bounded by configuration with spill support
- **Parallelization**: Automatic multi-threading via DataFusion
- **Format Efficiency**: Binary formats (Arrow, Parquet) for high throughput

This module bridges the gap between Feldera's incremental streaming processing and traditional batch SQL analytics, providing essential inspection and analysis capabilities for developers and operators.
<!-- SECTION:crates/adapters/src/adhoc/CLAUDE.md END -->

---

## Context: crates/adapters/src/controller/CLAUDE.md
<!-- SECTION:crates/adapters/src/controller/CLAUDE.md START -->
# Controller Architecture

The controller module implements the centralized orchestration layer for Feldera's I/O adapter system. It coordinates circuit execution, manages data flow, implements fault tolerance, and provides runtime statistics.

## Core Purpose

The controller serves as the **central control plane** that:
- Coordinates DBSP circuit execution with I/O endpoints
- Implements runtime flow control and backpressure management
- Manages pipeline state transitions and lifecycle
- Provides fault tolerance through journaling and checkpointing
- Collects and reports performance statistics
- Orchestrates streaming data ingestion and output processing

## Architecture Overview

### Multi-Threaded Design

The controller employs a **three-thread architecture** for optimal performance:

1. **Circuit Thread** (`CircuitThread`) - The main orchestrator that:
   - Owns the DBSP circuit handle and executes `circuit.step()`
   - Coordinates with input/output endpoints
   - Manages circuit state transitions (Running/Paused/Terminated)
   - Handles commands (checkpointing, profiling, suspension)
   - Implements fault-tolerant journaling and replay logic

2. **Backpressure Thread** (`BackpressureThread`) - Flow control manager that:
   - Monitors input buffer levels across all endpoints
   - Pauses/resumes input endpoints based on buffer thresholds
   - Prevents memory exhaustion during high-throughput scenarios
   - Implements user-requested connector state changes

3. **Statistics Thread** (`StatisticsThread`) - Performance monitoring that:
   - Collects metrics every second (memory, storage, processed records)
   - Maintains a 60-second rolling window of time series data
   - Notifies subscribers of new metrics via broadcast channels
   - Provides data for real-time monitoring and alerting

### Key Components

#### Controller (`Controller`)
- **Public API** for pipeline management and state control
- **Thread-safe wrapper** around `ControllerInner` with proper lifecycle management
- **Command interface** for asynchronous operations (checkpointing, profiling)

#### ControllerInner (`ControllerInner`)
- **Shared state** accessible by all threads
- **Input/Output endpoint management** with dynamic reconfiguration
- **Circuit catalog integration** for schema-aware processing
- **Configuration management** and validation

#### ControllerStatus (`ControllerStatus`)
- **Lock-free performance counters** using atomics for minimal overhead
- **Statistical aggregation** across all endpoints and circuit operations
- **Time series broadcasting** for real-time monitoring clients
- **Flow control metrics** for backpressure management

## File Organization

### Core Files

- **`mod.rs`** - Main controller implementation with multi-threaded orchestration
- **`stats.rs`** - Performance monitoring, metrics collection, and statistics reporting
- **`error.rs`** - Error handling abstractions (re-exports from adapterlib)

### Specialized Modules

- **`checkpoint.rs`** - Fault tolerance through persistent checkpointing
  - Circuit state serialization and recovery
  - Input endpoint offset tracking
  - Configuration preservation across restarts

- **`journal.rs`** - Transaction log for exactly-once processing
  - Step-by-step metadata recording
  - Replay mechanism for fault recovery
  - Storage-agnostic persistence layer

- **`sync.rs`** - Enterprise checkpoint synchronization (S3/cloud storage)
  - Distributed checkpoint management
  - Cross-instance state coordination
  - Activation markers for standby pipelines

- **`validate.rs`** - Configuration validation and dependency analysis
  - Connector dependency cycle detection
  - Pipeline configuration consistency checks
  - Input/output endpoint validation

- **`test.rs`** - Comprehensive testing utilities and integration tests

## Key Design Patterns

### 1. Lock-Free Statistics
```rust
// Atomic counters prevent blocking the datapath
pub total_input_records: AtomicU64,
pub total_processed_records: AtomicU64,
// Broadcast channels for real-time streaming
pub time_series_notifier: broadcast::Sender<SampleStatistics>,
```

**Benefits:**
- Zero-overhead metrics collection
- Non-blocking circuit execution
- Real-time monitoring without performance impact

### 2. Thread Coordination
```rust
struct CircuitThread {
    controller: Arc<ControllerInner>,      // Shared state
    circuit: DBSPHandle,                   // Circuit ownership
    backpressure_thread: BackpressureThread, // Flow control
    _statistics_thread: StatisticsThread,    // Metrics
}
```

**Benefits:**
- Clear separation of concerns
- Independent thread lifecycles
- Coordinated shutdown without deadlocks

### 3. Command Pattern for Async Operations
```rust
enum Command {
    GraphProfile(GraphProfileCallbackFn),
    Checkpoint(CheckpointCallbackFn),
    Suspend(SuspendCallbackFn),
}
```

**Benefits:**
- Non-blocking public API
- Type-safe callback handling
- Graceful error propagation

### 4. State Machine Pipeline Management
```rust
pub enum PipelineState {
    Paused,    // Endpoints paused, draining existing data
    Running,   // Active ingestion and processing
    Terminated, // Permanent shutdown
}
```

**Benefits:**
- Predictable state transitions
- Clean resource management
- User-controlled pipeline lifecycle

## Fault Tolerance Architecture

### Journaling System
- **Step-by-step logging** of all input processing
- **Metadata preservation** for exact replay scenarios
- **Storage-agnostic** design supporting file systems and cloud storage

### Checkpointing Integration
- **Circuit state snapshots** at configurable intervals
- **Input offset tracking** for consistent recovery points
- **Configuration preservation** across pipeline restarts

### Error Handling Strategy
- **Transient vs Permanent errors** with different recovery strategies
- **Graceful degradation** rather than complete failure
- **Detailed error context** for debugging and monitoring

## Performance Characteristics

### Throughput Optimization
- **Lock-free statistics** prevent contention on hot paths
- **Batched circuit steps** amortize processing overhead
- **Parallel I/O processing** with independent endpoint threads

### Memory Management
- **Bounded buffers** prevent unlimited memory growth
- **Backpressure coordination** maintains system stability
- **Efficient broadcast channels** for multiple monitoring clients

### Latency Minimization
- **Direct circuit coupling** avoids unnecessary queuing
- **Atomic metric updates** enable real-time monitoring
- **Thread affinity** for CPU cache optimization

## Integration Points

### DBSP Circuit Integration
- Direct `DBSPHandle` ownership and step execution
- Schema-aware input/output catalog management
- Performance profile collection and analysis

### Transport Layer Integration
- Dynamic endpoint creation and lifecycle management
- Pluggable transport implementations (Kafka, HTTP, Files)
- Format-agnostic data processing pipeline

### Storage System Integration
- Backend-agnostic checkpoint and journal persistence
- Cloud storage synchronization for distributed deployments
- Efficient binary serialization for compact storage

## Development Patterns

### Adding New Metrics
1. Define atomic counters in `ControllerStatus`
2. Update collection logic in `StatisticsThread`
3. Include in time series broadcasting
4. Add appropriate accessor methods

### Extending State Management
1. Modify `PipelineState` enum if needed
2. Update state transition logic in circuit thread
3. Add appropriate synchronization primitives
4. Update error handling and recovery paths

### Implementing New Commands
1. Add command variant to `Command` enum
2. Implement callback-based processing
3. Add public API method with proper error handling
4. Ensure graceful shutdown behavior

## Best Practices

### Thread Safety
- Use `Arc<AtomicBool>` for cancellation flags
- Employ `crossbeam` primitives for lock-free data structures
- Minimize shared mutable state through message passing

### Resource Management
- Implement proper `Drop` traits for cleanup
- Use `JoinHandle` for thread lifecycle management
- Employ timeouts for external operations

### Error Propagation
- Distinguish between recoverable and permanent errors
- Provide detailed error context for debugging
- Use structured error types with clear categorization

### Performance Monitoring
- Collect metrics without impacting datapath performance
- Use efficient serialization for network transmission
- Implement configurable aggregation windows

This controller architecture enables Feldera to achieve high-performance stream processing while maintaining strong consistency guarantees and providing comprehensive observability into pipeline operations.
<!-- SECTION:crates/adapters/src/controller/CLAUDE.md END -->

---

## Context: crates/adapters/src/format/CLAUDE.md
<!-- SECTION:crates/adapters/src/format/CLAUDE.md START -->
## Overview

The format module implements Feldera's unified data serialization/deserialization layer, providing extensible support for multiple data formats (JSON, CSV, Avro, Parquet, Raw) while integrating seamlessly with DBSP circuits. It enables high-performance data parsing and generation across diverse external systems while maintaining strong type safety and comprehensive error handling.

## Architecture Overview

### **Plugin-Based Factory Pattern**

The format system uses a registry-based architecture with runtime format discovery:

```rust
// Global format registries for input/output operations
static INPUT_FORMATS: LazyLock<IndexMap<String, Box<dyn InputFormat>>> = LazyLock::new(|| {
    let mut formats = IndexMap::new();
    formats.insert("json".to_string(), Box::new(JsonInputFormat) as _);
    formats.insert("csv".to_string(), Box::new(CsvInputFormat) as _);
    formats.insert("avro".to_string(), Box::new(AvroInputFormat) as _);
    formats.insert("parquet".to_string(), Box::new(ParquetInputFormat) as _);
    formats.insert("raw".to_string(), Box::new(RawInputFormat) as _);
    formats
});
```

### **Three-Layer Architecture**

#### **1. Format Factory Layer**
```rust
trait InputFormat: Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn new_parser(&self, endpoint_name: &str, input_stream: &InputCollectionHandle,
                  config: &YamlValue) -> Result<Box<dyn Parser>, ControllerError>;
}

trait OutputFormat: Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn new_encoder(&self, endpoint_name: &str, config: &ConnectorConfig,
                   key_schema: &Option<Relation>, value_schema: &Relation,
                   consumer: Box<dyn OutputConsumer>) -> Result<Box<dyn Encoder>, ControllerError>;
}
```

#### **2. Stream Processing Layer**
```rust
trait Parser: Send + Sync {
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>);
    fn splitter(&self) -> Box<dyn Splitter>;
    fn fork(&self) -> Box<dyn Parser>;
}

trait Encoder: Send {
    fn consumer(&mut self) -> &mut dyn OutputConsumer;
    fn encode(&mut self, batch: &dyn SerBatch) -> AnyResult<()>;
    fn fork(&self) -> Box<dyn Encoder>;
}
```

#### **3. Data Abstraction Layer**
```rust
trait InputBuffer: Send + Sync {
    fn flush(&mut self, handle: &InputCollectionHandle) -> Vec<ParseError>;
    fn take_some(&mut self, n: usize) -> Box<dyn InputBuffer>;
    fn hash(&self) -> u64;
    fn len(&self) -> usize;
}
```

## Format Implementations

### **JSON Format** (`json/`)

**Most Feature-Rich Implementation** supporting diverse update patterns:

#### **Update Format Variants**
```rust
pub enum JsonUpdateFormat {
    Raw,                    // Direct record insertion: {"field1": "value1", "field2": "value2"}
    InsertDelete,          // Explicit ops: {"insert": {record}, "delete": {record}}
    Debezium,              // CDC: {"payload": {"op": "c|u|d", "before": {..}, "after": {..}}}
    Snowflake,             // Streaming: {"action": "INSERT", "data": {record}}
    Redis,                 // Key-value: {"key": "value", "op": "SET"}
}
```

#### **Advanced JSON Processing**
- **JSON Splitter**: Sophisticated state machine handling nested objects and arrays
- **Schema Generation**: Kafka Connect JSON schema generation for Debezium integration
- **Error Recovery**: Field-level error attribution with suggested fixes
- **Array Support**: Both single objects and object arrays

**JSON Splitter State Machine**:
```rust
enum JsonSplitterState {
    Start,              // Looking for object/array start
    InObject(u32),      // Inside object, tracking nesting depth
    InArray(u32),       // Inside array, tracking nesting depth
    InString,           // Inside quoted string
    Escape,             // Handling escape sequences
}
```

### **CSV Format** (`csv/`)

**High-Performance Text Processing** with custom deserializer:

#### **Key Features**
- **Custom Deserializer**: Forked from `csv` crate to expose low-level `Deserializer`
- **Streaming Processing**: Line-by-line processing with configurable delimiters
- **Quote Handling**: Proper CSV quote escaping and multi-line field support
- **Header Support**: Optional header row processing and field mapping

#### **Configuration Options**
```rust
pub struct CsvParserConfig {
    pub delimiter: u8,              // Field delimiter (default: comma)
    pub quote: u8,                  // Quote character (default: double quote)
    pub escape: Option<u8>,         // Escape character
    pub has_headers: bool,          // First row contains headers
    pub flexible: bool,             // Allow variable field counts
    pub comment: Option<u8>,        // Comment line prefix
}
```

### **Avro Format** (`avro/`)

**Enterprise Integration Focus** with schema registry support:

#### **Schema Registry Integration**
```rust
pub struct AvroConfig {
    pub registry_url: Option<String>,           // Confluent Schema Registry URL
    pub authentication: SchemaRegistryAuth,     // Authentication configuration
    pub schema: Option<String>,                 // Inline schema definition
    pub schema_id: Option<u32>,                // Registry schema ID
    pub proxy: Option<String>,                 // HTTP proxy configuration
    pub timeout_ms: Option<u64>,               // Request timeout
}

pub enum SchemaRegistryAuth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
}
```

#### **Binary Processing**
- **Efficient Deserialization**: Direct Avro binary format processing
- **Schema Evolution**: Automatic handling of schema changes
- **Type Safety**: Strong typing with Rust's type system
- **Connection Pooling**: Reuse HTTP connections for schema registry access

### **Parquet Format** (`parquet/`)

**Analytics-Optimized Columnar Processing** with Arrow integration:

#### **Arrow Integration**
```rust
// Feldera SQL types → Arrow schema conversion
fn sql_to_arrow_type(sql_type: &SqlType) -> ArrowResult<DataType> {
    match sql_type {
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::TinyInt => Ok(DataType::Int8),
        SqlType::SmallInt => Ok(DataType::Int16),
        SqlType::Integer => Ok(DataType::Int32),
        SqlType::BigInt => Ok(DataType::Int64),
        SqlType::Decimal(precision, scale) => Ok(DataType::Decimal128(*precision as u8, *scale as i8)),
        // ... comprehensive type mapping
    }
}
```

#### **Batch Processing Architecture**
- **Columnar Efficiency**: Leverages Arrow's vectorized operations
- **Large Dataset Support**: Streaming processing of multi-GB files
- **Delta Lake Integration**: Special handling for Databricks Delta Lake format
- **Type Preservation**: Maintains SQL type semantics through Arrow conversion

### **Raw Format** (`raw.rs`)

**Minimal Overhead Processing** for binary data:

#### **Processing Modes**
```rust
pub enum RawReaderMode {
    Lines,      // Split input by newlines, each line becomes a record
    Blob,       // Entire input becomes a single record
}
```

#### **Use Cases**
- **Binary Data**: Direct processing of binary streams
- **Log Processing**: Line-based log file ingestion
- **Custom Protocols**: Raw byte stream handling for specialized formats
- **Zero Parsing**: Minimal CPU overhead for high-throughput scenarios

## Stream Processing Architecture

### **Data Flow Pipeline**

```
Raw Bytes → Splitter → Parser → InputBuffer → DBSP Circuit
    ↑           ↑        ↑         ↑           ↑
    |           |        |         |           |
Transport   Boundary   Record   Batched    Database
Layer       Detection  Parsing   Records    Operations
```

### **Stream Splitting Strategies**

#### **Format-Specific Splitters**
```rust
trait Splitter: Send {
    fn input(&mut self, data: &[u8]) -> Option<usize>;  // Returns boundary position
    fn clear(&mut self);                                // Reset internal state
}

// Different splitting strategies:
pub struct LineSplitter;        // Newline-based splitting (CSV, Raw)
pub struct JsonSplitter;        // JSON object/array boundary detection
pub struct Sponge;              // No splitting - consume entire input (Parquet)
```

#### **Boundary Detection**
- **Incremental Processing**: Handle partial records across buffer boundaries
- **State Preservation**: Maintain parsing state between buffer chunks
- **Memory Efficiency**: Process large streams without loading entire content

### **Buffer Management**

#### **InputBuffer Trait Implementation**
```rust
trait InputBuffer: Send + Sync {
    fn flush(&mut self, handle: &InputCollectionHandle) -> Vec<ParseError>;
    fn take_some(&mut self, n: usize) -> Box<dyn InputBuffer>;
    fn hash(&self) -> u64;                              // For replay consistency
    fn len(&self) -> usize;
}
```

#### **Advanced Buffer Features**
- **Partial Consumption**: `take_some()` for controlled batch processing
- **Replay Verification**: `hash()` for deterministic fault tolerance
- **Memory Management**: Efficient buffer allocation and reuse
- **Backpressure Handling**: Buffer size limits with overflow management

## Error Handling and Recovery

### **Structured Error System**

```rust
pub struct ParseError(Box<ParseErrorInner>);

struct ParseErrorInner {
    description: String,                    // Human-readable error description
    event_number: Option<u64>,             // Stream position for debugging
    field: Option<String>,                 // Failed field name
    invalid_bytes: Option<Vec<u8>>,        // Binary data that failed parsing
    invalid_text: Option<String>,          // Text data that failed parsing
    suggestion: Option<Cow<'static, str>>, // Suggested fix
}
```

### **Error Recovery Strategies**

#### **Graceful Degradation**
```rust
impl Parser for JsonParser {
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut buffer = JsonInputBuffer::new();
        let mut errors = Vec::new();

        // Continue processing after individual record failures
        for record in self.split_records(data) {
            match self.parse_record(record) {
                Ok(parsed) => buffer.push(parsed),
                Err(e) => {
                    errors.push(e);
                    // Continue with next record
                }
            }
        }

        (Some(Box::new(buffer)), errors)
    }
}
```

#### **Error Classification**
- **Field-Level Errors**: Specific field parsing failures with context
- **Record-Level Errors**: Entire record rejection with recovery suggestions
- **Batch-Level Errors**: Array parsing failures requiring batch rollback
- **Format-Level Errors**: Configuration or schema errors

## Type System Integration

### **Schema Conversion Pipeline**

#### **Unified Type Mapping**
```rust
// SQL types → Format-specific schema conversion
const JSON_SERDE_CONFIG: SqlSerdeConfig = SqlSerdeConfig {
    timestamp_format: TimestampFormat::String("%Y-%m-%d %H:%M:%S%.f".to_string()),
    date_format: DateFormat::String("%Y-%m-%d".to_string()),
    time_format: TimeFormat::String("%H:%M:%S%.f".to_string()),
    decimal_format: DecimalFormat::String,
    json_flavor: JsonFlavor::Default,
};
```

#### **Format-Specific Configurations**
- **JSON**: Flexible type coercion with null handling
- **CSV**: String-based parsing with configurable type inference
- **Avro**: Strong typing with schema evolution support
- **Parquet**: Arrow type system with SQL semantics preservation

### **Null Safety and Three-Valued Logic**

**Comprehensive Null Handling**:
```rust
// SQL NULL propagation through format layers
match field_value {
    Some(value) => parse_typed_value(value, field_type)?,
    None => Ok(SqlValue::Null),  // Preserve SQL null semantics
}
```

## Performance Optimization Patterns

### **Memory Management**

#### **Zero-Copy Processing**
```rust
// Direct byte slice processing where possible
fn parse_string_field(input: &[u8]) -> Result<&str, ParseError> {
    std::str::from_utf8(input).map_err(|e| ParseError::invalid_utf8(e))
}
```

#### **Buffer Reuse Patterns**
```rust
// Efficient buffer recycling
impl InputBuffer for JsonInputBuffer {
    fn take_some(&mut self, n: usize) -> Box<dyn InputBuffer> {
        let mut taken = Self::with_capacity(n);
        taken.records = self.records.drain(..n.min(self.records.len())).collect();
        Box::new(taken)
    }
}
```

### **Batch Processing Optimization**

#### **Configurable Batch Sizes**
- **Memory Constraints**: Limit batch sizes based on available memory
- **Latency Requirements**: Smaller batches for low-latency scenarios
- **Throughput Optimization**: Larger batches for maximum throughput

#### **Vectorized Operations**
- **Arrow Integration**: SIMD-optimized columnar operations for Parquet
- **Batch Type Conversion**: Vectorized SQL type conversions
- **Parallel Processing**: Multi-threaded parsing with `fork()` support

### **Network and I/O Optimization**

#### **Schema Registry Optimization**
```rust
// Connection pooling and caching
struct SchemaRegistryClient {
    client: Arc<reqwest::Client>,           // Reuse HTTP connections
    schema_cache: RwLock<HashMap<u32, Schema>>, // Cache schemas
    auth_token: Option<Arc<str>>,           // Cached authentication
}
```

## Testing Infrastructure

### **Comprehensive Test Strategy**

#### **Property-Based Testing**
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn json_parser_handles_arbitrary_input(input in ".*") {
        let parser = JsonParser::new(config);
        let (buffer, errors) = parser.parse(input.as_bytes());
        // Verify parsing never panics and errors are structured
        assert!(buffer.is_some() || !errors.is_empty());
    }
}
```

#### **Mock Infrastructure**
```rust
// Isolated testing with mock consumers
struct MockOutputConsumer {
    records: Vec<MockUpdate>,
    errors: Vec<ParseError>,
}

impl OutputConsumer for MockOutputConsumer {
    fn batch_end(&mut self) -> AnyResult<()> {
        // Capture batch boundaries for testing
    }
}
```

### **Format Compatibility Testing**

#### **Round-Trip Verification**
```rust
#[test]
fn test_json_roundtrip() {
    let original_data = generate_test_records();

    // Serialize with encoder
    let encoded = JsonEncoder::encode(&original_data)?;

    // Parse with parser
    let (parsed_buffer, errors) = JsonParser::parse(&encoded)?;

    // Verify data integrity
    assert_eq!(original_data, parsed_buffer.records);
    assert!(errors.is_empty());
}
```

## Configuration and Setup Patterns

### **Hierarchical Configuration System**

#### **Multi-Source Configuration**
```rust
// HTTP Request → Format Configuration
fn config_from_http_request(&self, endpoint_name: &str, request: &HttpRequest)
    -> Result<Box<dyn ErasedSerialize>, ControllerError> {

    // Extract format-specific configuration from HTTP headers/body
    let config = self.extract_config_from_request(request)?;
    self.validate_config(&config)?;
    Ok(Box::new(config))
}

// YAML Configuration → Parser Instance
fn new_parser(&self, endpoint_name: &str, input_stream: &InputCollectionHandle,
              config: &YamlValue) -> Result<Box<dyn Parser>, ControllerError> {

    let parsed_config = self.parse_yaml_config(config)?;
    self.validate_parser_config(&parsed_config)?;
    Ok(Box::new(self.create_parser(parsed_config)))
}
```

#### **Configuration Validation**
- **Schema Validation**: Early validation of format configurations
- **Transport Compatibility**: Format-transport compatibility verification
- **Resource Constraints**: Memory and performance limit validation
- **Security Validation**: Authentication and authorization checks

## Integration Patterns

### **Transport Layer Integration**

#### **Transport-Agnostic Design**
The format layer maintains complete independence from transport specifics:
- **Abstract Interfaces**: `InputConsumer`/`OutputConsumer` for transport integration
- **Configuration Extraction**: HTTP, YAML, and programmatic configuration sources
- **Buffer Negotiation**: Transport-specific buffer size and batching limits

### **DBSP Circuit Integration**

#### **Streaming Data Flow**
```rust
// Direct integration with DBSP circuits
impl InputBuffer for FormatInputBuffer {
    fn flush(&mut self, handle: &InputCollectionHandle) -> Vec<ParseError> {
        let mut errors = Vec::new();

        // Stream records directly to DBSP circuit
        for record in self.records.drain(..) {
            match handle.insert(record) {
                Ok(_) => {},
                Err(e) => errors.push(ParseError::circuit_error(e)),
            }
        }

        errors
    }
}
```

## Development Best Practices

### **Adding New Format Support**

1. **Create Format Module**: Add new module under `format/`
2. **Implement Core Traits**: `InputFormat`, `OutputFormat`, `Parser`, `Encoder`
3. **Add Stream Splitter**: Implement boundary detection logic
4. **Type System Integration**: Map format types to SQL types
5. **Configuration Support**: Add YAML and HTTP configuration parsing
6. **Comprehensive Testing**: Unit tests, integration tests, property tests
7. **Documentation**: Update format documentation and examples
8. **Registry Integration**: Add to `INPUT_FORMATS`/`OUTPUT_FORMATS` registries

### **Performance Tuning Guidelines**

#### **Memory Optimization**
- **Profile Memory Usage**: Use `cargo bench` and memory profiling tools
- **Optimize Hot Paths**: Focus on frequently executed parsing code
- **Buffer Size Tuning**: Configure optimal buffer sizes for workload
- **Connection Pooling**: Reuse network connections where possible

#### **Throughput Optimization**
- **Batch Size Tuning**: Balance latency vs throughput requirements
- **Parallel Processing**: Leverage multi-core processing with `fork()`
- **Zero-Copy Patterns**: Avoid unnecessary data copying
- **SIMD Integration**: Use vectorized operations for columnar formats

### **Error Handling Best Practices**

#### **User-Friendly Errors**
- **Precise Error Messages**: Include field names, positions, and suggested fixes
- **Error Attribution**: Link errors back to original data sources
- **Graceful Recovery**: Continue processing after recoverable errors
- **Rich Context**: Provide sufficient context for debugging

This format module represents a sophisticated, production-ready data processing system that successfully abstracts the complexity of multiple data formats while providing high performance, reliability, and extensibility for the Feldera streaming analytics platform.
<!-- SECTION:crates/adapters/src/format/CLAUDE.md END -->

---

## Context: crates/adapters/src/integrated/CLAUDE.md
<!-- SECTION:crates/adapters/src/integrated/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build with all integrated connector features
cargo build -p adapters --features="with-deltalake,with-iceberg"

# Run PostgreSQL connector tests
cargo test -p adapters postgres

# Run Delta Lake connector tests (requires AWS credentials)
cargo test -p adapters delta_table

# Run Iceberg connector tests
cargo test -p adapters --features="iceberg-tests-fs" iceberg

# Build specific integrated connector
cargo build -p adapters --features="with-deltalake"
```

### Feature Flags

```toml
# Available integrated connector features
with-deltalake = ["deltalake"]        # Delta Lake support
with-iceberg = ["feldera-iceberg"]    # Apache Iceberg support
# PostgreSQL support is always enabled via tokio-postgres
```

## Architecture Overview

### Technology Stack

- **Database Integration**: PostgreSQL with tokio-postgres
- **Data Lake Integration**: Delta Lake with deltalake crate, Apache Iceberg
- **Query Engine**: DataFusion for Delta Lake filtering and projection
- **Storage Backends**: S3, Azure Blob, Google Cloud Storage
- **Data Formats**: Parquet for Delta Lake, custom serialization for PostgreSQL
- **Async Runtime**: Tokio for non-blocking I/O operations

### Core Purpose

The Integrated Connectors module provides **tightly-coupled transport and format implementations** for external data systems:

- **Single-Purpose Connectors**: Transport protocol and data format are integrated into one component
- **Database CDC**: Real-time change data capture from PostgreSQL
- **Data Lake Integration**: Batch and streaming access to Delta Lake and Iceberg tables
- **Format Optimization**: Native format handling without separate format layer
- **Schema Evolution**: Support for evolving schemas in data lake formats

## Project Structure

### Core Components

#### Main Module (`mod.rs`)
- Factory functions for creating integrated endpoints
- Trait definitions for `IntegratedInputEndpoint` and `IntegratedOutputEndpoint`
- Feature-gated module imports

#### PostgreSQL Integration (`postgres/`)
- **Input**: SQL query execution and result streaming
- **Output**: INSERT, UPSERT, DELETE operations with prepared statements
- **Features**: Type mapping, connection pooling, error retry logic

#### Delta Lake Integration (`delta_table/`)
- **Input**: Table scanning, incremental reads, change data feed (CDF)
- **Output**: Parquet file writing with Delta Lake transaction log
- **Features**: S3/Azure/GCS storage, schema evolution, time travel

#### Apache Iceberg Integration
- **External crate**: `feldera-iceberg` (referenced but implementation in separate crate)
- **Features**: Schema evolution, partition management, catalog integration

## Implementation Details

### Integrated Endpoint Architecture

#### Core Traits
```rust
// From mod.rs - integrated output endpoint combining transport and encoding
pub trait IntegratedOutputEndpoint: OutputEndpoint + Encoder {
    fn into_encoder(self: Box<Self>) -> Box<dyn Encoder>;
    fn as_endpoint(&mut self) -> &mut dyn OutputEndpoint;
}

// Factory function for creating integrated endpoints
pub fn create_integrated_output_endpoint(
    endpoint_id: EndpointId,
    endpoint_name: &str,
    connector_config: &ConnectorConfig,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Weak<ControllerInner>,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError>
```

#### Format Integration Validation
```rust
// Integrated connectors don't allow separate format specification
if connector_config.format.is_some() {
    return Err(ControllerError::invalid_parser_configuration(
        endpoint_name,
        &format!("{} transport does not allow 'format' specification",
                connector_config.transport.name())
    ));
}
```

### PostgreSQL Integration

#### Input Connector - Query Execution
```rust
// From postgres/input.rs - SQL query execution and streaming
pub struct PostgresInputEndpoint {
    inner: Arc<PostgresInputEndpointInner>,
}

impl PostgresInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &PostgresReaderConfig,    // Contains SQL query and connection info
        consumer: Box<dyn InputConsumer>,
    ) -> Self
}
```

#### Connection Management
```rust
// From postgres/input.rs - PostgreSQL connection handling
async fn connect_to_postgres(&self)
    -> Result<(Client, Connection<Socket, NoTlsStream>), ControllerError> {

    let (client, connection) = tokio_postgres::connect(self.config.uri.as_str(), NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok((client, connection))
}
```

#### Type Mapping Implementation
```rust
// From postgres/input.rs - comprehensive PostgreSQL to JSON type mapping
let value = match *col_type {
    Type::BOOL => {
        let v: Option<bool> = row.get(col_idx);
        json!(v)
    }
    Type::VARCHAR | Type::TEXT => {
        let v: Option<String> = row.get(col_idx);
        json!(v)
    }
    Type::INT4 => {
        let v: Option<i32> = row.get(col_idx);
        json!(v)
    }
    Type::TIMESTAMP => {
        let v: Option<chrono::NaiveDateTime> = row.get(col_idx);
        let vutc = v.as_ref().map(|v| Utc.from_utc_datetime(v).to_rfc3339());
        json!(vutc)
    }
    Type::NUMERIC => {
        let v: Option<Decimal> = row.get(col_idx);
        json!(v)
    }
    // Support for arrays, UUIDs, and other PostgreSQL types...
};
```

#### Output Connector - Prepared Statements
```rust
// From postgres/output.rs - prepared statement management
struct PreparedStatements {
    insert: Statement,
    upsert: Statement,    // INSERT ... ON CONFLICT DO UPDATE
    delete: Statement,
}

// Error classification for retry logic
enum BackoffError {
    Temporary(anyhow::Error),    // Retry with backoff
    Permanent(anyhow::Error),    // Fail immediately
}

impl From<postgres::Error> for BackoffError {
    fn from(value: postgres::Error) -> Self {
        // Classify connection errors as temporary, SQL errors as permanent
        if value.is_closed() || matches!(value.code(), Some(SqlState::CONNECTION_FAILURE)) {
            Self::Temporary(anyhow!("failed to connect to postgres: {value}"))
        } else {
            Self::Permanent(anyhow!("postgres error: permanent: {value}"))
        }
    }
}
```

### Delta Lake Integration

#### Input Connector - Table Scanning
```rust
// From delta_table/input.rs - Delta Lake table scanning
pub struct DeltaTableInputEndpoint {
    inner: Arc<DeltaTableInputEndpointInner>,
}

impl DeltaTableInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &DeltaTableReaderConfig,  // Table URI, query filters, etc.
        consumer: Box<dyn InputConsumer>,
    ) -> Self
}
```

#### Storage Handler Registration
```rust
// From delta_table/mod.rs - cloud storage integration
static REGISTER_STORAGE_HANDLERS: Once = Once::new();

pub fn register_storage_handlers() {
    REGISTER_STORAGE_HANDLERS.call_once(|| {
        deltalake::aws::register_handlers(None);      // S3 support
        deltalake::azure::register_handlers(None);    // Azure Blob support
        deltalake::gcp::register_handlers(None);      // Google Cloud Storage
    });
}
```

#### Delta Lake-Specific Serialization
```rust
// From delta_table/mod.rs - Delta Lake serialization configuration
pub fn delta_input_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        // Delta uses microsecond timestamps in Parquet
        .with_timestamp_format(TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%Z"))
        .with_date_format(DateFormat::DaysSinceEpoch)
        .with_decimal_format(DecimalFormat::String)
        .with_uuid_format(UuidFormat::String)  // UUIDs as strings in Delta
}
```

#### DataFusion Integration for Query Pushdown
```rust
// From delta_table/input.rs - DataFusion integration for filtering
use datafusion::prelude::{SessionContext, DataFrame};
use deltalake::datafusion::logical_expr::Expr;

// Create DataFusion context for Delta Lake
let session_ctx = SessionContext::new();
let delta_table = DeltaTableBuilder::from_uri(&config.uri).build().await?;

// Apply filtering at storage layer for performance
if let Some(filter_expr) = &config.filter {
    let logical_plan = session_ctx
        .read_table(Arc::new(delta_table))?
        .filter(filter_expr.clone())?
        .logical_plan().clone();
}
```

### Multi-Format Support Patterns

#### Feature-Gated Endpoint Creation
```rust
// From mod.rs - conditional compilation based on features
pub fn create_integrated_input_endpoint(
    endpoint_name: &str,
    config: &ConnectorConfig,
    consumer: Box<dyn InputConsumer>,
) -> Result<Box<dyn IntegratedInputEndpoint>, ControllerError> {

    let ep: Box<dyn IntegratedInputEndpoint> = match &config.transport {
        #[cfg(feature = "with-deltalake")]
        DeltaTableInput(config) => Box::new(DeltaTableInputEndpoint::new(
            endpoint_name, config, consumer
        )),

        #[cfg(feature = "with-iceberg")]
        IcebergInput(config) => Box::new(feldera_iceberg::IcebergInputEndpoint::new(
            endpoint_name, config, consumer
        )),

        PostgresInput(config) => Box::new(PostgresInputEndpoint::new(
            endpoint_name, config, consumer
        )),

        transport => Err(ControllerError::unknown_input_transport(
            endpoint_name, &transport.name()
        ))?,
    };

    Ok(ep)
}
```

## Key Features and Capabilities

### PostgreSQL Integration
- **Query-based Input**: Execute arbitrary SQL queries and stream results
- **Comprehensive Type Support**: All PostgreSQL types including arrays and UUIDs
- **Change Streaming**: Real-time data ingestion from query results
- **Prepared Statements**: Optimized INSERT/UPSERT/DELETE operations
- **Connection Management**: Automatic reconnection and error recovery
- **Transaction Safety**: ACID compliance for output operations

### Delta Lake Integration
- **Schema Evolution**: Handle schema changes across table versions
- **Time Travel**: Read data from specific table versions or timestamps
- **Change Data Feed**: Incremental processing of table changes
- **Multi-Cloud Storage**: S3, Azure Blob, Google Cloud Storage support
- **Partition Pruning**: Efficient reading through partition elimination
- **DataFusion Optimization**: Query pushdown and column projection

### Apache Iceberg Integration
- **Catalog Integration**: Support for Hive, Glue, REST catalogs
- **Schema Evolution**: Advanced schema evolution capabilities
- **Partition Management**: Automatic partition lifecycle management
- **Multi-Engine Compatibility**: Compatible with Spark, Flink, Trino

## Configuration and Usage

### PostgreSQL Configuration
```yaml
# Input connector - execute query and stream results
transport:
  name: postgres_input
  config:
    uri: "postgresql://user:pass@localhost:5432/database"
    query: "SELECT id, name, created_at FROM users WHERE created_at > NOW() - INTERVAL '1 day'"

# Output connector - write to table
transport:
  name: postgres_output
  config:
    uri: "postgresql://user:pass@localhost:5432/database"
    table: "processed_events"
    mode: "upsert"  # insert, upsert, or append
```

### Delta Lake Configuration
```yaml
# Input connector - read from Delta Lake table
transport:
  name: delta_table_input
  config:
    uri: "s3://my-bucket/delta-tables/events/"
    mode: "snapshot"  # snapshot or follow
    version: 42  # optional: read specific version
    timestamp: "2024-01-15T10:00:00Z"  # optional: read at timestamp

# Output connector - write to Delta Lake
transport:
  name: delta_table_output
  config:
    uri: "s3://my-bucket/delta-tables/processed/"
    mode: "append"  # append or overwrite
```

## Development Workflow

### Adding New Integrated Connector

1. **Create connector module** in `src/integrated/my_connector/`
2. **Implement required traits**:
   - `IntegratedInputEndpoint` for input connectors
   - `IntegratedOutputEndpoint` for output connectors
3. **Add feature flag** in `Cargo.toml`
4. **Update factory functions** in `mod.rs`
5. **Add configuration types** in `feldera-types`
6. **Implement comprehensive tests** with real external services

### Testing Strategy

#### Unit Tests
- Type conversion and serialization correctness
- Error handling and retry logic
- Configuration validation

#### Integration Tests
- Real database/storage system connectivity
- End-to-end data flow validation
- Performance and reliability testing
- Schema evolution scenarios

### Performance Optimization

#### Connection Management
- **Connection Pooling**: Reuse database connections
- **Async I/O**: Non-blocking operations throughout
- **Batch Processing**: Optimize throughput with batching
- **Retry Logic**: Exponential backoff for transient failures

#### Data Processing
- **Schema Caching**: Cache schema information to avoid repeated lookups
- **Columnar Processing**: Leverage Arrow/Parquet columnar formats
- **Query Pushdown**: Push filters and projections to storage layer
- **Parallel Processing**: Multi-threaded data processing where possible

## Dependencies and Integration

### Core Dependencies
- **tokio-postgres** - PostgreSQL async client
- **deltalake** - Delta Lake Rust implementation with DataFusion
- **feldera-iceberg** - Apache Iceberg integration (separate crate)
- **datafusion** - Query engine for Delta Lake optimization
- **arrow** - Columnar data processing

### Cloud Storage Dependencies
- **AWS SDK** - S3 integration for Delta Lake
- **Azure SDK** - Azure Blob Storage support
- **Google Cloud SDK** - Google Cloud Storage support

### Error Handling Patterns
- **Classified Errors**: Distinguish between temporary and permanent failures
- **Retry Logic**: Exponential backoff for transient errors
- **Circuit Breakers**: Prevent cascade failures
- **Graceful Degradation**: Continue operation when possible

### Security Considerations
- **Connection Encryption**: TLS for database connections
- **Cloud Authentication**: IAM roles and service accounts
- **Credential Management**: Secure credential storage and rotation
- **Access Control**: Fine-grained permissions for data access

## Best Practices

### Configuration Design
- **Sensible Defaults**: Provide reasonable default values
- **Validation**: Validate configuration at startup
- **Documentation**: Clear documentation for all configuration options
- **Feature Flags**: Use feature flags for optional dependencies

### Error Handling
- **Structured Errors**: Use detailed error types with context
- **User-Friendly Messages**: Provide actionable error messages
- **Logging**: Comprehensive logging for debugging
- **Metrics**: Export metrics for monitoring and alerting

### Performance Monitoring
- **Throughput Metrics**: Track records/second processed
- **Latency Metrics**: Monitor end-to-end processing latency
- **Error Rates**: Track and alert on error rates
- **Resource Usage**: Monitor CPU, memory, and network usage

This module enables Feldera to integrate seamlessly with external data systems while maintaining high performance and reliability through native format handling and optimized data processing pipelines.
<!-- SECTION:crates/adapters/src/integrated/CLAUDE.md END -->

---

## Context: crates/adapters/src/transport/CLAUDE.md
<!-- SECTION:crates/adapters/src/transport/CLAUDE.md START -->
## Overview

The transport module implements Feldera's I/O abstraction layer, providing unified access to diverse external systems for DBSP circuits. It enables high-performance, fault-tolerant data ingestion and emission across transports including Kafka, HTTP, files, databases, and cloud storage systems, while maintaining strong consistency guarantees for incremental computation.

## Architecture Overview

### **Dual Transport Abstraction Model**

The transport system uses two complementary abstraction patterns:

#### **Regular Transports** (Transport + Format Separation)
```rust
TransportInputEndpoint → InputReader → Parser → InputConsumer
```
- **Transport**: Handles data movement and connection management
- **Format**: Separate parser handles data serialization/deserialization
- **Examples**: HTTP, Kafka, File, URL, S3, PubSub, Redis

#### **Integrated Transports** (Transport + Format Combined)
```rust
IntegratedInputEndpoint → InputReader → InputConsumer
```
- **Unified**: Transport and format tightly coupled for efficiency
- **Examples**: PostgreSQL, Delta Lake, Iceberg (database-specific optimizations)

### **Core Abstractions and Traits**

#### Primary Transport Traits
```rust
pub trait InputEndpoint {
    fn endpoint_name(&self) -> &str;
    fn supports_fault_tolerance(&self) -> bool;
}

pub trait TransportInputEndpoint: InputEndpoint {
    fn open(&self, consumer: Box<dyn InputConsumer>,
           parser: Box<dyn Parser>, ...) -> Box<dyn InputReader>;
}

pub trait IntegratedInputEndpoint: InputEndpoint {
    fn open(self: Box<Self>, input_handle: &InputCollectionHandle,
           ...) -> Box<dyn InputReader>;
}
```

#### Input Reader Interface
```rust
pub trait InputReader: Send {
    fn seek(&mut self, position: JsonValue) -> AnyResult<()>;
    fn request(&mut self, command: InputReaderCommand);
    fn is_closed(&self) -> bool;
}
```

**Command-Driven State Management**:
```rust
pub enum InputReaderCommand {
    Queue,      // Start queuing data
    Extend,     // Resume normal processing
    Pause,      // Pause data ingestion
    Replay { seek, queue, hash }, // Replay from checkpoint
    Disconnect, // Graceful shutdown
}
```

## Transport Implementations

### **HTTP Transport** (`http/`)

**Key Features**:
- **Real-time Streaming**: Chunked transfer encoding with configurable timeouts
- **Exactly-Once Support**: Fault tolerance with seek/replay capabilities
- **State Management**: Atomic operations for thread-safe state transitions
- **Flexible Endpoints**: GET, POST, and streaming endpoint support

**Architecture Pattern**:
```rust
// Background worker thread pattern
thread::spawn(move || {
    while let Some(command) = command_receiver.recv() {
        match command {
            Extend => start_streaming_data(),
            Pause => pause_and_queue(),
            Disconnect => graceful_shutdown(),
        }
    }
});
```

**Configuration**:
```rust
HttpInputConfig {
    path: String,           // HTTP endpoint URL
    method: HttpMethod,     // GET/POST request method
    timeout_ms: Option<u64>, // Request timeout
    headers: BTreeMap<String, String>, // Custom headers
}
```

### **Kafka Transport** (`kafka/`)

**Dual Implementation Strategy**:

#### **Fault-Tolerant Kafka** (`ft/`):
- **Exactly-Once Semantics**: Transaction-based processing with commit coordination
- **Complex State Management**: Offset tracking with replay capabilities
- **Memory Monitoring**: Real-time memory usage reporting for large datasets
- **Error Handling**: Sophisticated retry logic with exponential backoff

#### **Non-Fault-Tolerant Kafka** (`nonft/`):
- **High Performance**: Simplified processing for maximum throughput
- **At-Least-Once**: Basic reliability without exact replay guarantees
- **Reduced Overhead**: Minimal state tracking for performance-critical scenarios

**Key Implementation Details**:
```rust
// Kafka-specific error refinement
fn refine_kafka_error<C>(client: &KafkaClient<C>, e: KafkaError) -> (bool, AnyError) {
    // Converts librdkafka errors into actionable Anyhow errors
    // Determines fatality for proper error handling strategy
}

// Authentication integration
KafkaAuthConfig::Aws { region } => {
    // AWS MSK IAM authentication with credential chain
}
```

### **File Transport** (`file.rs`)

**File Processing Features**:
- **Follow Mode**: Tail-like behavior for continuously growing files
- **Seek Support**: Resume from specific file positions using byte offsets
- **Buffer Management**: Configurable read buffers for memory efficiency
- **Testing Integration**: Barrier synchronization for deterministic test execution

**Seek Implementation**:
```rust
impl InputReader for FileInputReader {
    fn seek(&mut self, position: JsonValue) -> AnyResult<()> {
        if let JsonValue::Number(n) = position {
            self.file.seek(SeekFrom::Start(n.as_u64().unwrap()))?;
        }
    }
}
```

### **URL Transport** (`url.rs`)

**Advanced HTTP Features**:
- **Range Requests**: HTTP Range header support for resumable downloads
- **Connection Management**: Automatic reconnection with configurable timeouts
- **Pause/Resume**: Sophisticated state management with timeout handling
- **Error Recovery**: Retry logic with exponential backoff and circuit breaking

**Resume Capability**:
```rust
// HTTP Range request for resumption
let range_header = format!("bytes={}-", current_position);
request.headers.insert("Range", range_header);
```

### **Specialized Transports**

#### **S3 Transport** (`s3.rs`):
- **AWS SDK Integration**: Native AWS authentication and region support
- **Object Streaming**: Efficient streaming of large S3 objects
- **Prefix Filtering**: Support for processing multiple objects with key patterns

#### **Redis Transport** (`redis.rs`):
- **Multiple Data Structures**: Support for streams, lists, and pub/sub patterns
- **Connection Pooling**: Efficient connection reuse and management
- **Auth Support**: Redis AUTH and ACL integration

#### **Ad Hoc Transport** (`adhoc.rs`):
- **Direct Data Injection**: Programmatic data insertion for testing and development
- **Schema Flexibility**: Support for arbitrary data structures
- **Development Support**: Simplified data injection for rapid prototyping

## Error Handling Architecture

### **Hierarchical Error Classification**

**Error Severity Levels**:
```rust
// Fatal errors require complete restart
if is_fatal_error(&error) {
    consumer.error(true, error); // Signal fatal condition
    break; // Terminate processing loop
}

// Non-fatal errors allow recovery
consumer.error(false, error); // Continue processing
```

**Error Context Preservation**:
- **Rich Error Messages**: Actionable information with suggested fixes
- **Source Location**: Precise error location tracking for debugging
- **Error Chains**: Maintain full error context through transformation layers

### **Async Error Handling**

**Out-of-Band Error Reporting**:
```rust
// Background thread error callback
let error_callback = Arc::clone(&consumer);
tokio::spawn(async move {
    if let Err(e) = async_operation().await {
        error_callback.error(false, e.into());
    }
});
```

## Concurrency and Threading Patterns

### **Thread-per-Transport Architecture**

Most transports follow a consistent threading pattern:

```rust
let (command_sender, command_receiver) = unbounded();
let worker_thread = thread::Builder::new()
    .name(format!("{}-worker", transport_name))
    .spawn(move || {
        transport_worker_loop(command_receiver, consumer)
    })?;

// InputReader implementation delegates to worker thread
struct TransportInputReader {
    command_sender: UnboundedSender<InputReaderCommand>,
    worker_handle: JoinHandle<()>,
}
```

### **Channel-Based Communication**

**Command Dispatch Pattern**:
- **Unbounded Channels**: `UnboundedSender<InputReaderCommand>` for command dispatch
- **Backpressure Handling**: Optional bounded channels for flow control
- **Error Propagation**: Separate error channels for out-of-band error reporting

### **Mixed Async/Sync Design**

**Tokio Integration Strategy**:
```rust
// Background thread with Tokio runtime for async operations
let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;

rt.block_on(async {
    // Async HTTP client operations
    let response = http_client.get(url).send().await?;
    // Process streaming response
});
```

## Fault Tolerance and State Management

### **Three-Tier Fault Tolerance Model**

#### **Level 1: None**
- No persistence or recovery capabilities
- Suitable for non-critical or easily reproducible data sources
- Example: Development/testing scenarios

#### **Level 2: At-Least-Once**
- Can resume from checkpoints but may have duplicates
- Checkpoint-based recovery with seek capability
- Example: File reading with position tracking

#### **Level 3: Exactly-Once**
- Can replay exact data with hash verification
- Strong consistency guarantees for critical data processing
- Example: Kafka transactions with offset management

### **Resume/Replay Implementation**

```rust
pub enum Resume {
    Barrier,                              // Cannot resume - start from beginning
    Seek { seek: JsonValue },            // Resume from checkpoint position
    Replay { seek: JsonValue, queue: Vec<ParsedStream>, hash: u64 }, // Exact replay
}

impl InputReader for FaultTolerantReader {
    fn request(&mut self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Replay { seek, queue, hash } => {
                self.seek(seek)?;
                self.replay_queue(queue, hash)?; // Exact data replay
                self.resume_normal_processing();
            }
        }
    }
}
```

### **Journaling and Checkpointing**

**Metadata Persistence**:
- Position tracking for seekable transports
- Queue snapshots for exact replay scenarios
- Hash verification for data integrity validation

## Configuration and Factory Patterns

### **Unified Configuration System**

All transport configurations are centralized in `feldera-types/src/transport/`:

```rust
#[derive(Deserialize, Serialize, Clone)]
pub enum TransportConfig {
    Kafka(KafkaInputConfig),
    Http(HttpInputConfig),
    File(FileInputConfig),
    // ... other transport configs
}
```

### **Factory Pattern Implementation**

```rust
pub fn input_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    secrets_dir: &Path,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {

    match config {
        TransportConfig::Kafka(kafka_config) => {
            let endpoint = KafkaInputEndpoint::new(kafka_config, secrets_dir)?;
            Ok(Some(Box::new(endpoint)))
        }
        TransportConfig::Http(http_config) => {
            let endpoint = HttpInputEndpoint::new(http_config)?;
            Ok(Some(Box::new(endpoint)))
        }
        // ... other transport factories
    }
}
```

### **Secret Management Integration**

**Secure Credential Handling**:
```rust
// Resolve secrets from files or environment
let resolved_config = config.resolve_secrets(secrets_dir)?;
let credentials = extract_credentials(&resolved_config)?;
```

## Testing Infrastructure and Patterns

### **Barrier-Based Test Synchronization**

**Deterministic Test Execution**:
```rust
#[cfg(test)]
static BARRIERS: Mutex<BTreeMap<String, usize>> = Mutex::new(BTreeMap::new());

pub fn set_barrier(name: &str, value: usize) {
    BARRIERS.lock().unwrap().insert(name.into(), value);
}

pub fn barrier_wait(name: &str) {
    // Synchronization point for deterministic test execution
    while BARRIERS.lock().unwrap().get(name).copied().unwrap_or(0) > 0 {
        thread::sleep(Duration::from_millis(10));
    }
}
```

### **Mock Transport Implementations**

**Test Transport Pattern**:
```rust
struct MockInputReader {
    data: Vec<ParsedStream>,
    position: usize,
    consumer: Box<dyn InputConsumer>,
}

impl InputReader for MockInputReader {
    fn request(&mut self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Extend => self.send_next_batch(),
            InputReaderCommand::Pause => self.pause_processing(),
        }
    }
}
```

### **Integration Testing**

**Real External Service Testing**:
- Docker-based test environments for Kafka, PostgreSQL, Redis
- Testcontainers integration for isolated testing
- Property-based testing for fault tolerance scenarios

## Performance Optimization Patterns

### **Batching and Buffering**

**Efficient Data Processing**:
```rust
// Configurable batch sizes for optimal throughput
const DEFAULT_BATCH_SIZE: usize = 1000;

// Buffer management for memory efficiency
struct BufferedReader {
    buffer: Vec<u8>,
    batch_size: usize,
    max_buffer_size: usize,
}
```

### **Memory Management**

**Resource Monitoring**:
```rust
// Memory usage tracking for large datasets
pub fn report_memory_usage(&self) -> MemoryUsage {
    MemoryUsage {
        buffered_bytes: self.buffer.len(),
        queued_records: self.queue.len(),
        peak_memory: self.peak_memory.load(Ordering::Relaxed),
    }
}
```

### **Connection Pooling and Reuse**

**Efficient Resource Utilization**:
- HTTP connection pooling with keep-alive
- Kafka connection reuse across multiple topics
- Database connection pooling for integrated transports

## Development Best Practices

### **Adding New Transport Implementation**

1. **Define Configuration**: Add transport config to `TransportConfig` enum
2. **Implement Traits**: Create `TransportInputEndpoint` and `InputReader` implementations
3. **Error Handling**: Implement proper error classification and recovery
4. **Threading**: Follow thread-per-transport pattern with command channels
5. **Testing**: Add comprehensive unit and integration tests
6. **Documentation**: Update transport documentation and examples

### **Debugging Transport Issues**

**Diagnostic Tools**:
- **Logging**: Comprehensive tracing at debug/trace levels
- **Metrics**: Built-in performance and error rate monitoring
- **State Inspection**: Runtime state examination capabilities
- **Error Analysis**: Rich error context with actionable information

### **Performance Tuning**

**Optimization Areas**:
- **Buffer Sizes**: Tune based on data characteristics and memory constraints
- **Thread Configuration**: Optimize worker thread count for I/O patterns
- **Connection Pooling**: Configure pool sizes based on workload patterns
- **Batching**: Optimize batch sizes for throughput vs. latency trade-offs

This transport layer provides Feldera with a robust, high-performance, and extensible I/O foundation that enables reliable data processing at scale while maintaining the strong consistency guarantees required for incremental computation.
<!-- SECTION:crates/adapters/src/transport/CLAUDE.md END -->

---

## Context: crates/CLAUDE.md
<!-- SECTION:crates/CLAUDE.md START -->
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
<!-- SECTION:crates/CLAUDE.md END -->

---

## Context: crates/datagen/CLAUDE.md
<!-- SECTION:crates/datagen/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the datagen crate
cargo build -p datagen

# Run tests
cargo test -p datagen

# Use as library in other crates
cargo run --example generate_test_data -p datagen
```

## Architecture Overview

### Technology Stack

- **Random Generation**: Pseudo-random and deterministic data generation
- **Configurable**: Flexible data distribution and patterns
- **Performance**: High-throughput data generation
- **Testing**: Support for unit and integration testing

### Core Purpose

Data Generation provides **test data generation utilities**:

- **Realistic Data**: Generate realistic test datasets
- **Configurable Distributions**: Control data patterns and distributions
- **Performance Testing**: High-volume data generation for benchmarks
- **Reproducible**: Deterministic generation for testing

### Project Structure

#### Core Module

- `src/lib.rs` - Main data generation library and utilities

## Important Implementation Details

### Data Generation Framework

#### Core Traits
```rust
pub trait DataGenerator<T> {
    fn generate(&mut self) -> T;
    fn generate_batch(&mut self, count: usize) -> Vec<T>;
}

pub trait ConfigurableGenerator<T, C> {
    fn new(config: C) -> Self;
    fn with_seed(config: C, seed: u64) -> Self;
}
```

#### Generation Features
- **Seeded Random**: Deterministic generation with seeds
- **Batch Generation**: Efficient bulk data creation
- **Custom Distributions**: Normal, uniform, zipf, and custom distributions
- **Correlated Data**: Generate related data with realistic correlations

### Data Types Support

#### Supported Types
- **Numeric**: Integers, floats with various distributions
- **Strings**: Random strings, names, emails, addresses
- **Dates**: Timestamps with realistic patterns
- **Geographic**: Coordinates, addresses, regions
- **Business**: Customer IDs, transaction data, product information

#### Realistic Patterns
```rust
// Generate realistic customer data
pub struct CustomerGenerator {
    name_gen: NameGenerator,
    email_gen: EmailGenerator,
    age_dist: NormalDistribution,
}

impl CustomerGenerator {
    pub fn generate_customer(&mut self) -> Customer {
        let name = self.name_gen.generate();
        let email = self.email_gen.generate_from_name(&name);
        let age = self.age_dist.sample();

        Customer { name, email, age }
    }
}
```

### Performance Optimization

#### High-Throughput Generation
- **Batch Processing**: Generate data in efficient batches
- **Memory Pooling**: Reuse memory allocations
- **SIMD**: Vector operations for numeric generation
- **Caching**: Cache expensive computations

#### Memory Management
- **Zero-Copy**: Minimize data copying where possible
- **Streaming**: Generate data on-demand without storing
- **Bounded Memory**: Control memory usage for large datasets
- **Cleanup**: Automatic resource cleanup

## Development Workflow

### For New Data Types

1. Define data structure and generation parameters
2. Implement `DataGenerator` trait
3. Add realistic distribution patterns
4. Add configuration options
5. Test with various parameters
6. Add performance benchmarks

### For Custom Distributions

1. Implement distribution algorithm
2. Add statistical validation
3. Test distribution properties
4. Document distribution parameters
5. Add examples and use cases
6. Benchmark performance characteristics

### Testing Strategy

#### Correctness Testing
- **Distribution Testing**: Validate statistical properties
- **Boundary Testing**: Test edge cases and limits
- **Correlation Testing**: Verify data relationships
- **Determinism**: Test reproducible generation

#### Performance Testing
- **Throughput**: Measure generation rate
- **Memory Usage**: Monitor memory consumption
- **Scaling**: Test with various data sizes
- **Efficiency**: Compare with baseline implementations

### Configuration System

#### Generation Configuration
```rust
pub struct DataGenConfig {
    pub seed: Option<u64>,
    pub batch_size: usize,
    pub distribution: DistributionType,
    pub correlation_strength: f64,
}

pub enum DistributionType {
    Uniform { min: f64, max: f64 },
    Normal { mean: f64, std_dev: f64 },
    Zipf { exponent: f64 },
    Custom(Box<dyn Distribution>),
}
```

#### Flexible Configuration
- **Multiple Sources**: Code, files, environment variables
- **Validation**: Validate configuration parameters
- **Defaults**: Sensible default values
- **Documentation**: Document all configuration options

### Usage Examples

#### Basic Generation
```rust
use datagen::{DataGenerator, UniformIntGenerator};

let mut gen = UniformIntGenerator::new(1, 100);
let values: Vec<i32> = gen.generate_batch(1000);
```

#### Realistic Data
```rust
use datagen::{CustomerGenerator, TransactionGenerator};

let mut customer_gen = CustomerGenerator::with_seed(42);
let mut transaction_gen = TransactionGenerator::new();

for _ in 0..1000 {
    let customer = customer_gen.generate();
    let transactions = transaction_gen.generate_for_customer(&customer, 1..10);
}
```

### Configuration Files

- `Cargo.toml` - Dependencies for random generation and statistics
- Minimal external dependencies for broad compatibility

### Dependencies

#### Core Dependencies
- `rand` - Random number generation
- `rand_distr` - Statistical distributions
- `chrono` - Date/time generation
- `uuid` - UUID generation

### Best Practices

#### Generation Design
- **Realistic**: Generate data that resembles real-world patterns
- **Configurable**: Allow customization of generation parameters
- **Deterministic**: Support seeded generation for reproducible tests
- **Efficient**: Optimize for high-throughput generation

#### API Design
- **Trait-Based**: Use traits for extensible generation
- **Batch-Friendly**: Support efficient batch generation
- **Memory-Aware**: Consider memory usage patterns
- **Error-Free**: Avoid panics in generation code

#### Testing Integration
- **Test Utilities**: Provide utilities for common testing patterns
- **Fixtures**: Generate standard test fixtures
- **Scenarios**: Support various testing scenarios
- **Validation**: Include data validation utilities

### Integration Patterns

#### With Testing Frameworks
```rust
#[cfg(test)]
mod tests {
    use datagen::TestDataGenerator;

    #[test]
    fn test_with_generated_data() {
        let data = TestDataGenerator::new()
            .with_size(100)
            .with_pattern(Pattern::Realistic)
            .generate();

        assert!(validate_data(&data));
    }
}
```

#### With Benchmarks
```rust
fn bench_processing(b: &mut Bencher) {
    let data = generate_benchmark_data(10_000);
    b.iter(|| {
        process_data(&data)
    });
}
```

This crate provides essential data generation capabilities that support testing, benchmarking, and development across the entire Feldera platform.
<!-- SECTION:crates/datagen/CLAUDE.md END -->

---

## Context: crates/dbsp/CLAUDE.md
<!-- SECTION:crates/dbsp/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the DBSP crate
cargo build -p dbsp

# Run tests
cargo test -p dbsp

# Run specific test
cargo test -p dbsp test_name

# Run benchmarks
cargo bench -p dbsp

# Run examples
cargo run --example degrees
cargo run --example tutorial1
```

### Development Tools

```bash
# Check with clippy
cargo clippy -p dbsp

# Format code
cargo fmt -p dbsp

# Generate documentation
cargo doc -p dbsp --open

# Run with memory sanitizer (requires nightly)
cargo +nightly test -p dbsp --target x86_64-unknown-linux-gnu -Zbuild-std --features=sanitizer
```

## Architecture Overview

### Technology Stack

- **Language**: Rust with advanced type system features
- **Concurrency**: Multi-threaded execution with worker pools
- **Memory Management**: Custom allocators with mimalloc integration
- **Serialization**: rkyv for zero-copy serialization
- **Testing**: Extensive property-based testing with proptest

### Core Concepts

DBSP is a computational engine for **incremental computation** on changing datasets:

- **Incremental Processing**: Changes propagate in time proportional to change size, not dataset size
- **Stream Processing**: Continuous analysis of changing data
- **Differential Computation**: Maintains differences between consecutive states
- **Circuit Model**: Computation expressed as circuits of operators

### Project Structure

#### Core Directories

- `src/circuit/` - Circuit infrastructure and runtime
- `src/operator/` - Computational operators (map, filter, join, aggregate, etc.)
- `src/trace/` - Data structures for storing and indexing collections
- `src/algebra/` - Mathematical abstractions (lattices, groups, etc.)
- `src/dynamic/` - Dynamic typing system for runtime flexibility
- `src/storage/` - Persistent storage backend
- `src/time/` - Timestamp and ordering abstractions

#### Key Components

- **Runtime**: Circuit execution engine with scheduling
- **Operators**: Computational primitives (50+ operators)
- **Traces**: Indexed data structures for efficient querying
- **Handles**: Input/output interfaces for circuits

## Important Implementation Details

### Circuit Model

```rust
use dbsp::{Runtime, OutputHandle, IndexedZSet};

// Create circuit with 2 worker threads
let (mut circuit, handles) = Runtime::init_circuit(2, |circuit| {
    let (input, handle) = circuit.add_input_zset::<(String, i32), isize>();
    let output = input.map(|(k, v)| (k.clone(), v * 2));
    Ok((handle, output.output()))
})?;

// Execute one step
circuit.step()?;
```

### Operator Categories

#### Core Operators
- **Map/Filter**: Element-wise transformations
- **Aggregation**: Group-by and windowing operations
- **Join**: Various join algorithms (hash, indexed, asof)
- **Time Series**: Windowing and temporal operations

#### Advanced Operators
- **Recursive**: Fixed-point computations
- **Dynamic**: Runtime-configurable operators
- **Communication**: Multi-worker coordination
- **Storage**: Persistent state management

### Memory Management

- **Custom Allocators**: mimalloc for performance
- **Zero-Copy**: rkyv serialization avoids allocations
- **Batch Processing**: Amortized allocation costs
- **Garbage Collection**: Automatic cleanup of unused data

## Development Workflow

### For Operator Development

1. Define operator in `src/operator/`
2. Implement required traits (`Operator`, `StrictOperator`, etc.)
3. Add comprehensive tests in module
4. Add property tests with proptest
5. Update documentation and examples

### For Algorithm Implementation

1. Study existing patterns in `src/operator/`
2. Consider incremental vs non-incremental versions
3. Implement both typed and dynamic variants if needed
4. Add benchmarks for performance validation
5. Test with various data distributions

### Testing Strategy

#### Unit Tests
- Located alongside implementation code
- Test both correctness and incremental behavior
- Use `proptest` for property-based testing

#### Integration Tests
- Complex multi-operator scenarios
- End-to-end pipeline testing
- Performance regression detection

#### Benchmarks
- Located in `benches/` directory
- Real-world datasets (fraud detection, social networks)
- Performance tracking across versions

### Performance Considerations

#### Optimization Techniques
- **Batch Processing**: Process multiple elements together
- **Index Selection**: Choose appropriate data structures
- **Memory Layout**: Optimize for cache performance
- **Parallelization**: Leverage multi-core execution

#### Profiling Tools
- Built-in performance monitoring
- CPU profiling integration
- Memory usage tracking
- Visual circuit graph generation

### Configuration Files

- `Cargo.toml` - Package configuration with extensive feature flags
- `Makefile.toml` - Build automation scripts
- `benches/` - Performance benchmark suite
- `proptest-regressions/` - Property test regression data

### Key Features and Flags

- `default` - Standard feature set
- `with-serde` - Serialization support
- `persistence` - Persistent storage backend
- `mimalloc` - High-performance allocator
- `tokio` - Async runtime integration

### Dependencies

#### Core Dependencies
- `rkyv` - Zero-copy serialization
- `crossbeam` - Lock-free data structures
- `hashbrown` - Fast hash maps
- `num-traits` - Numeric abstractions

#### Development Dependencies
- `proptest` - Property-based testing
- `criterion` - Benchmarking framework
- `tempfile` - Temporary file management

### Advanced Features

#### Dynamic Typing
- Runtime type flexibility
- Operator composition at runtime
- Schema evolution support

#### Storage Backend
- Persistent state management
- Checkpoint/recovery mechanisms
- File-based and cloud storage options

#### Multi-threading
- Automatic work distribution
- Lock-free data structures
- NUMA-aware scheduling

### Best Practices

- **Incremental First**: Always consider incremental behavior
- **Type Safety**: Leverage Rust's type system for correctness
- **Performance**: Profile before optimizing
- **Testing**: Property tests for complex invariants
- **Documentation**: Comprehensive examples and tutorials
<!-- SECTION:crates/dbsp/CLAUDE.md END -->

---

## Context: crates/dbsp/src/CLAUDE.md
<!-- SECTION:crates/dbsp/src/CLAUDE.md START -->
## Overview

The `crates/dbsp/src/` directory contains the core implementation of DBSP (Database Stream Processor), a computational engine for incremental computation on changing datasets. DBSP enables processing changes in time proportional to the size of changes rather than the entire dataset, making it ideal for continuous analysis of large, frequently-changing data.

## Architecture Overview

### **Computational Model**

DBSP is based on a formal theoretical foundation that provides:

1. **Semantics**: Formal language of streaming operators with precise stream transformation specifications
2. **Algorithm**: Incremental dataflow program generation that processes input events proportional to input size rather than database state size

### **Core Design Principles**

- **Incremental Processing**: Changes propagate through circuits in time proportional to change size, not dataset size
- **Stream Processing**: Continuous analysis of changing data through operator circuits
- **Differential Computation**: Maintains differences between consecutive states using mathematical foundations
- **Circuit Model**: Computation expressed as circuits of interconnected operators with formal semantics

## Directory Structure Overview

### **Direct Subdirectories of `dbsp/src/`**

#### **`algebra/`**
Mathematical foundations and algebraic structures underlying DBSP's incremental computation model. Contains abstract algebraic concepts including monoids, groups, rings, and lattices that provide the theoretical basis for change propagation and differential computation. The `zset/` subdirectory implements Z-sets (collections with multiplicities) which are fundamental to representing insertions and deletions in incremental computation. Includes specialized number types (`checked_int.rs`, `floats.rs`) and ordering abstractions (`order.rs`, `lattice.rs`) essential for maintaining mathematical correctness in streaming operations.

#### **`circuit/`**
Core circuit infrastructure providing the runtime execution engine for DBSP computations. Contains circuit construction (`circuit_builder.rs`), execution control (`runtime.rs`), and scheduling (`schedule/`) components. The `dbsp_handle.rs` provides the main API for multi-worker circuit execution, while `checkpointer.rs` handles state persistence for fault tolerance. Includes performance monitoring (`metrics.rs`), circuit introspection (`trace.rs`), and integration with async runtimes (`tokio.rs`). The scheduler supports dynamic work distribution with NUMA-aware thread management.

#### **`dynamic/`**
Dynamic typing system that enables runtime flexibility while maintaining performance. Implements trait object architecture to avoid excessive monomorphization during compilation. Contains core dynamic types (`data.rs`, `pair.rs`, `vec.rs`), serialization support (`rkyv.rs`), and factory patterns (`factory.rs`) for creating trait objects. The `erase.rs` module handles type erasure, while `downcast.rs` provides safe downcasting mechanisms. Essential for SQL compiler integration where concrete types are not known at compile time.

#### **`monitor/`**
Circuit monitoring and visualization tools for debugging and performance analysis. Provides circuit graph generation (`circuit_graph.rs`) for visual representation of operator connectivity and data flow. The `visual_graph.rs` module creates GraphViz-compatible output for circuit visualization. Essential for understanding complex circuit behavior, debugging performance bottlenecks, and validating circuit construction correctness during development.

#### **`operator/`**
Complete implementation of DBSP's operator library containing 50+ streaming operators. Organized into basic operators (`apply.rs`, `filter_map.rs`, `plus.rs`), aggregation operators (`aggregate.rs`, `group/`), join operators (`join.rs`, `asof_join.rs`, `semijoin.rs`), and specialized operators (`time_series/`, `recursive.rs`). The `dynamic/` subdirectory provides dynamically-typed versions of all operators for runtime flexibility. Communication operators (`communication/`) handle multi-worker data distribution. Input/output handling (`input.rs`, `output.rs`) provides type-safe interfaces for data ingestion and emission.

#### **`profile/`**
Performance profiling and monitoring infrastructure for circuit execution analysis. Contains CPU profiling support (`cpu.rs`) with integration to standard Rust profiling tools. Provides DBSP-specific performance counters, memory usage tracking, and execution time analysis. Essential for performance optimization, identifying bottlenecks, and ensuring efficient resource utilization in production deployments.

#### **`storage/`**
Multi-tier persistent storage system supporting various backend implementations. The `backend/` directory contains storage abstractions with memory (`memory_impl.rs`) and POSIX file system (`posixio_impl.rs`) implementations. Buffer caching (`buffer_cache/`) provides intelligent memory management with LRU eviction policies. File format handling (`file/`) implements zero-copy serialization with rkyv, while `dirlock/` provides file system synchronization. Supports both memory-optimized and disk-based storage for different deployment scenarios.

#### **`time/`**
Temporal abstractions and time-based computation support for streaming operations. Implements timestamp types and ordering relationships (`antichain.rs`, `product.rs`) essential for maintaining temporal consistency in incremental computation. Provides the foundation for time-based operators, windowing operations, and watermark handling. Critical for ensuring correct temporal semantics in streaming analytics and maintaining consistency across distributed computations.

#### **`trace/`**
Core data structures for storing and indexing collections with efficient access patterns. Contains batch and trace implementations (`ord/`) supporting both in-memory and file-based storage. The `cursor/` subdirectory provides iteration interfaces, while `layers/` implements hierarchical data organization. Spine-based temporal organization (`spine_async/`) enables efficient merge operations for time-ordered data. Supports various specialized trace types including key-value batches, weighted sets, and indexed collections optimized for different query patterns.

#### **`utils/`**
Utility functions and helper modules supporting core DBSP functionality. Contains sorting algorithms (`sort.rs`, `unstable_sort.rs`), data consolidation (`consolidation/`), and specialized data structures. Tuple generation (`tuple/`) provides compile-time tuple creation, while `vec_ext.rs` extends vector functionality. The `sample.rs` module implements sampling algorithms for statistical operations. Includes property-based testing utilities and performance optimization helpers.

## Module Architecture

### **Core Infrastructure Layers**

#### **1. Dynamic Dispatch System** (`dynamic/`)
**Purpose**: Provides dynamic typing to limit monomorphization and balance compilation speed with runtime performance.

**Key Components**:
```rust
// Core trait hierarchy for dynamic dispatch
trait Data: Clone + Eq + Ord + Hash + SizeOf + Send + Sync + Debug + 'static {}
trait DataTrait: DowncastTrait + ClonableTrait + ArchiveTrait + Data {}

// Factory pattern for creating trait objects
trait Factory<T> {
    fn default_box(&self) -> Box<T>;
    fn default_ref(&self) -> &mut T;
}
```

**Dynamic Type System**:
- `DynData`: Dynamically typed data with concrete type erasure
- `DynPair`: Dynamic tuples for key-value relationships
- `DynVec`: Dynamic vectors with efficient batch operations
- `DynWeightedPairs`: Collections of weighted data for incremental computation

**Safety Considerations**: Uses unsafe code for performance by eliding TypeId checks in release builds while maintaining type safety through careful API design.

#### **2. Algebraic Foundations** (`algebra/`)
**Purpose**: Mathematical abstractions underlying incremental computation.

**Core Algebraic Structures**:
```rust
// Trait hierarchy for mathematical structures
trait SemigroupValue: Clone + Eq + SizeOf + AddByRef + 'static {}
trait MonoidValue: SemigroupValue + HasZero {}
trait GroupValue: MonoidValue + Neg<Output = Self> + NegByRef {}
trait RingValue: GroupValue + Mul<Output = Self> + MulByRef<Output = Self> + HasOne {}
```

**Z-Set Implementation** (`algebra/zset/`):
- **ZSet**: Collections with multiplicities (positive/negative weights)
- **IndexedZSet**: Indexed collections for efficient lookups and joins
- **Mathematical Operations**: Addition, subtraction, multiplication following group theory
- **Change Propagation**: Differential computation using +/- weights for insertions/deletions

#### **3. Trace System** (`trace/`)
**Purpose**: Core data structures for storing and indexing collections with time-based organization.

**Batch and Trace Abstractions**:
```rust
trait BatchReader {
    type Key: DBData;
    type Val: DBData;
    type Time: Timestamp;
    type Diff: MonoidValue;

    fn cursor(&self) -> Self::Cursor;
}

trait Trace: BatchReader {
    fn append_batch(&mut self, batch: &Self::Batch);
    fn map_batches<F>(&self, f: F);
}
```

**Storage Implementations**:
- **In-Memory**: `VecBatch`, `OrdBatch` for fast access
- **File-Based**: `FileBatch` for persistent storage with memory efficiency
- **Fallback**: `FallbackBatch` combining memory and file storage
- **Spine**: Efficient merge trees for temporal data organization

#### **4. Circuit Infrastructure** (`circuit/`)
**Purpose**: Runtime execution engine with scheduling, state management, and multi-threading.

**Circuit Execution Model**:
```rust
trait Operator {
    fn eval(&mut self);
    fn commit(&mut self);
    fn get_metadata(&self) -> OperatorMeta;
}

// Circuit construction and execution
let (circuit, handles) = Runtime::init_circuit(workers, |circuit| {
    let input = circuit.add_input_zset::<Record, Weight>();
    let output = input.map(|r| transform(r));
    Ok((input_handle, output.output()))
})?;
```

**Key Circuit Components**:
- **Runtime**: Multi-worker execution with NUMA-aware scheduling
- **Scheduler**: Dynamic scheduling with work-stealing parallelization
- **Handles**: Type-safe input/output interfaces (`InputHandle`, `OutputHandle`)
- **Checkpointing**: State persistence for fault tolerance
- **Metrics**: Performance monitoring and circuit introspection

## Operator System

### **Operator Categories and Implementations**

#### **Core Transformation Operators** (`operator/`)

**Basic Operators**:
```rust
// Element-wise transformations
.map(|x| f(x))           // Map operator
.filter(|x| p(x))        // Filter operator
.filter_map(|x| f(x))    // Combined filter and map

// Arithmetic operations
.plus(&other)            // Set union with weight addition
.minus(&other)           // Set difference with weight subtraction
```

**Aggregation Operators**:
```rust
// Group-by aggregation with incremental maintenance
.aggregate_generic::<K, V, R>(
    |k, v| key_func(k, v),          // Key extraction
    |acc, k, v, w| agg_func(acc, k, v, w), // Aggregation function
)

// Window aggregation
.window_aggregate(window_spec, agg_func)
```

**Join Operators**:
```rust
// Hash join with incremental updates
.join::<K, V1, V2>(&other, |k, v1, v2| result(k, v1, v2))

// Index join for efficient lookups
.index_join(&indexed_stream, join_key)

// As-of join for temporal relationships
.asof_join(&other, time_key, join_condition)
```

#### **Advanced Operators**

**Recursive Computation**:
```rust
// Fixed-point iteration for recursive queries
circuit.recursive(|child| {
    let (feedback, output) = child.add_feedback(z_set_factory);
    let result = base_case.plus(&feedback.delay().recursive_step());
    feedback.connect(&result);
    Ok(output)
})
```

**Time Series Operators** (`operator/time_series/`):
- **Windowing**: Time-based and row-based windows with expiration
- **Watermarks**: Late data handling and progress tracking
- **Rolling Aggregates**: Efficient sliding window computations
- **Range Queries**: Time range-based data retrieval

**Communication Operators** (`operator/communication/`):
- **Exchange**: Data redistribution across workers
- **Gather**: Collecting distributed results
- **Shard**: Partitioning data for parallel processing

### **Dynamic vs Static APIs**

#### **Static API** (Top-level `operator/` modules)
```rust
// Type-safe, compile-time checked operations
let result: Stream<_, OrdZSet<(String, i32), isize>> = input
    .map(|(k, v)| (k.to_uppercase(), v * 2))
    .filter(|(_, v)| *v > 0);
```

#### **Dynamic API** (`operator/dynamic/`)
```rust
// Runtime-flexible operations with dynamic typing
let result = input_dyn
    .map_generic(&map_factory, &output_factory)
    .filter_generic(&filter_factory);
```

**Trade-offs**:
- **Static**: Better performance, compile-time type checking, less flexibility
- **Dynamic**: Faster compilation, runtime flexibility, SQL compiler integration

## Storage Architecture

### **Multi-Tier Storage System** (`storage/`)

#### **Storage Backends** (`storage/backend/`)
```rust
trait StorageBackend {
    fn create_file(&self, path: &StoragePath) -> Result<Self::File, StorageError>;
    fn open_file(&self, path: &StoragePath) -> Result<Self::File, StorageError>;
}

// Implementations:
// - MemoryBackend: In-memory for testing and small datasets
// - PosixBackend: File system storage for production
```

#### **Buffer Cache System** (`storage/buffer_cache/`)
- **LRU Caching**: Intelligent buffer management with configurable cache sizes
- **Async I/O**: Non-blocking file operations with efficient prefetching
- **Memory Mapping**: Direct memory access for large files when beneficial
- **Cache Statistics**: Performance monitoring and cache hit rate tracking

#### **File Format and Serialization** (`storage/file/`)
```rust
// Zero-copy serialization with rkyv
trait Serializable {
    fn serialize<S: Serializer>(&self, serializer: &mut S) -> Result<(), S::Error>;
}

// File-based batch implementations
FileBatch {
    reader: FileReader<K, V, T, R>,
    metadata: BatchMetadata,
    cache_stats: CacheStats,
}
```

## Performance Architecture

### **Multi-Threading and Parallelization**

#### **Worker-Based Execution Model**:
```rust
// Multi-worker runtime with work-stealing scheduler
let workers = 8;
let (circuit, handles) = Runtime::init_circuit(workers, circuit_constructor)?;

// Automatic work distribution across operators
// NUMA-aware memory allocation
// Lock-free data structures for synchronization
```

#### **Operator Parallelization**:
- **Embarrassingly Parallel**: Map, filter operations across workers
- **Hash-Based Partitioning**: Join operations with consistent hashing
- **Pipeline Parallelism**: Different operators executing concurrently
- **Batch Processing**: Amortized costs through efficient batching

### **Memory Management Optimization**

#### **Custom Allocation Strategies**:
```rust
// mimalloc integration for high-performance allocation
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
```

#### **Zero-Copy Techniques**:
- **rkyv Serialization**: Zero-copy deserialization for file I/O
- **Reference Sharing**: `Rc`/`Arc` for immutable data sharing
- **In-Place Updates**: Mutation when safe for performance
- **Batch Reuse**: Buffer recycling to minimize allocations

### **Optimization Techniques**

#### **Data Structure Selection**:
- **Indexed Collections**: B-trees for sorted data with range queries
- **Hash Tables**: Hash maps for point lookups and equi-joins
- **Vectors**: Sequential access patterns with cache efficiency
- **Sparse Representations**: Efficient storage for sparse datasets

#### **Algorithm Optimization**:
- **Incremental Algorithms**: Differential computation for all operators
- **Index Selection**: Automatic index creation for query optimization
- **Lazy Evaluation**: Defer computation until results are needed
- **Batch Consolidation**: Merge operations for efficiency

## Testing and Validation

### **Testing Architecture**

#### **Property-Based Testing**:
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_incremental_correctness(
        initial_data in vec((any::<String>(), any::<i32>()), 0..100),
        updates in vec((any::<String>(), any::<i32>()), 0..50)
    ) {
        // Verify incremental result matches batch computation
        let incremental_result = incremental_computation(&initial_data, &updates);
        let batch_result = batch_computation(&[&initial_data[..], &updates[..]].concat());
        prop_assert_eq!(incremental_result, batch_result);
    }
}
```

#### **Integration Testing**:
- **End-to-End Pipelines**: Complete circuit execution validation
- **Correctness Verification**: Incremental vs batch result comparison
- **Performance Regression**: Benchmark tracking across versions
- **Fault Tolerance**: Checkpoint/recovery scenario testing

### **Debugging and Profiling**

#### **Circuit Introspection** (`monitor/`):
```rust
// Visual circuit graph generation
circuit.generate_graphviz(&mut output);

// Performance metrics collection
let metrics = circuit.gather_metrics();
println!("Operator throughput: {} records/sec", metrics.throughput);
```

#### **Profiling Integration** (`profile/`):
- **CPU Profiling**: Integration with standard Rust profiling tools
- **Memory Analysis**: Allocation tracking and memory usage patterns
- **Custom Metrics**: DBSP-specific performance counters
- **Tracing Support**: Distributed tracing for complex circuits

## Development Patterns and Best Practices

### **Operator Development Guidelines**

#### **Implementing New Operators**:
1. **Define Traits**: Specify operator behavior through trait definitions
2. **Static Implementation**: Create type-safe static version first
3. **Dynamic Wrapper**: Add dynamic dispatch for SQL compiler integration
4. **Testing**: Comprehensive unit tests with property-based validation
5. **Documentation**: Examples and performance characteristics

#### **Performance Optimization**:
- **Profile First**: Use built-in profiling before optimizing
- **Batch Processing**: Prefer batch operations over single-record processing
- **Memory Layout**: Consider cache-friendly data arrangements
- **Incremental Logic**: Ensure algorithms process only changes when possible

### **Integration Points**

#### **SQL Compiler Integration**:
- Dynamic operators provide runtime flexibility for compiled SQL
- Factory pattern enables type-erased circuit construction
- Metadata system supports query optimization and introspection

#### **Storage System Integration**:
- Pluggable storage backends for different deployment scenarios
- Checkpointing support for fault-tolerant long-running computations
- File-based batches for memory-efficient large dataset processing

### **Error Handling Patterns**

#### **Structured Error Types**:
```rust
#[derive(Debug, Error)]
pub enum Error {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Runtime error: {0}")]
    Runtime(#[from] RuntimeError),

    #[error("Scheduler error: {0}")]
    Scheduler(#[from] SchedulerError),
}
```

#### **Recovery Strategies**:
- **Graceful Degradation**: Continue processing when possible
- **State Restoration**: Checkpoint-based recovery for critical errors
- **Error Propagation**: Structured error context through computation stack

This DBSP source code represents a sophisticated computational engine that successfully implements incremental computation theory in a high-performance, production-ready system. The modular architecture, extensive use of Rust's type system, and careful performance optimization create a powerful foundation for streaming analytics and incremental view maintenance.
<!-- SECTION:crates/dbsp/src/CLAUDE.md END -->

---

## Context: crates/fda/CLAUDE.md
<!-- SECTION:crates/fda/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the fda crate
cargo build -p fda

# Run the FDA CLI tool
cargo run -p fda

# Run interactive shell
cargo run -p fda -- shell

# Run benchmarks
cargo run -p fda -- bench

# Test bash integration
./test.bash
```

## Architecture Overview

### Technology Stack

- **CLI Framework**: Command-line interface for development tasks
- **Interactive Shell**: REPL-style development environment
- **Benchmarking**: Performance testing utilities
- **API Integration**: OpenAPI specification and testing

### Core Purpose

FDA provides **development tools and utilities** for Feldera:

- **CLI Commands**: Development workflow automation
- **Interactive Shell**: Exploratory development environment
- **Benchmarking Tools**: Performance measurement and testing
- **API Testing**: OpenAPI specification validation

### Project Structure

#### Core Modules

- `src/main.rs` - CLI entry point and argument parsing
- `src/cli.rs` - Command-line interface implementation
- `src/shell.rs` - Interactive shell and REPL
- `src/bench/` - Benchmarking utilities and API
- `src/adhoc.rs` - Ad-hoc development utilities

## Important Implementation Details

### CLI Interface

#### Command Structure
```rust
#[derive(Parser)]
pub enum Command {
    Shell,
    Bench(BenchArgs),
    Adhoc(AdhocArgs),
}
```

#### CLI Features
- **Subcommands**: Organized development tasks
- **Interactive Mode**: Shell-like development environment
- **Configuration**: Flexible configuration options
- **Help System**: Comprehensive help and documentation

### Interactive Shell

#### Shell Features
```rust
pub struct Shell {
    history: Vec<String>,
    context: ShellContext,
}

impl Shell {
    pub fn run(&mut self) -> Result<(), ShellError>;
    pub fn execute_command(&mut self, cmd: &str) -> Result<String, ShellError>;
}
```

#### Shell Capabilities
- **Command History**: Navigate previous commands
- **Tab Completion**: Smart command completion
- **Context Awareness**: Maintain development context
- **Script Execution**: Run shell scripts

### Benchmarking System

#### Benchmark API
```rust
pub struct BenchmarkSuite {
    tests: Vec<BenchmarkTest>,
    config: BenchConfig,
}

impl BenchmarkSuite {
    pub fn run(&self) -> BenchmarkResults;
    pub fn add_test(&mut self, test: BenchmarkTest);
}
```

#### Benchmarking Features
- **Performance Testing**: Measure execution time and throughput
- **Statistical Analysis**: Confidence intervals and variance
- **Comparison**: Compare against baseline performance
- **Reporting**: Detailed benchmark reports

### OpenAPI Integration

#### API Testing
```rust
pub struct ApiTester {
    spec: OpenApiSpec,
    client: HttpClient,
}

impl ApiTester {
    pub fn validate_spec(&self) -> Result<(), ValidationError>;
    pub fn test_endpoints(&self) -> Result<TestResults, TestError>;
}
```

#### API Features
- **Spec Validation**: OpenAPI specification validation
- **Endpoint Testing**: Automated endpoint testing
- **Schema Validation**: Request/response schema validation
- **Documentation Generation**: API documentation updates

## Development Workflow

### Using the CLI

#### Development Tasks
```bash
# Start interactive development session
fda shell

# Run performance benchmarks
fda bench --suite performance

# Execute ad-hoc development tasks
fda adhoc --task generate_test_data
```

#### Shell Commands
```bash
# In interactive shell
> help                    # Show available commands
> bench list              # List available benchmarks
> api validate            # Validate API specifications
> test run integration    # Run integration tests
```

### For New CLI Commands

1. Add command to `Command` enum in `src/cli.rs`
2. Implement command handler function
3. Add argument parsing with clap
4. Add help text and examples
5. Test command with various inputs
6. Update documentation

### For New Benchmarks

1. Add benchmark test to `src/bench/`
2. Define benchmark parameters and metrics
3. Implement benchmark execution logic
4. Add statistical analysis and reporting
5. Test with various workloads
6. Document benchmark purpose and interpretation

### Testing Strategy

#### CLI Testing
```bash
# Test CLI commands
./test.bash

# Test interactive shell
echo "help\nquit" | fda shell

# Test benchmarks
fda bench --dry-run
```

#### Integration Testing
- **End-to-End**: Complete workflow testing
- **API Integration**: Test with real Feldera services
- **Performance**: Validate benchmark accuracy
- **Error Handling**: Test error conditions

### Configuration

#### CLI Configuration
```rust
pub struct FdaConfig {
    pub default_endpoint: String,
    pub benchmark_config: BenchConfig,
    pub shell_config: ShellConfig,
}
```

#### Configuration Sources
- **Config Files**: TOML configuration files
- **Environment Variables**: Runtime configuration
- **Command Line**: Override configuration options
- **Interactive**: Set options in shell mode

### Build Configuration

#### Build Script Integration
```rust
// build.rs
fn main() {
    // Generate CLI completion scripts
    generate_completions();

    // Build benchmark assets
    build_bench_assets();
}
```

#### Features
- **Completion Scripts**: Shell completion generation
- **Asset Bundling**: Embed benchmark data
- **OpenAPI Integration**: API specification handling

### Configuration Files

- `Cargo.toml` - CLI and benchmarking dependencies
- `build.rs` - Build-time asset generation
- `test.bash` - Integration test script
- `bench_openapi.json` - OpenAPI specification for testing

### Dependencies

#### Core Dependencies
- `clap` - Command-line argument parsing
- `rustyline` - Interactive readline support
- `serde` - Configuration serialization
- `tokio` - Async runtime for API calls

#### Benchmarking Dependencies
- `criterion` - Statistical benchmarking
- `reqwest` - HTTP client for API testing
- `openapi` - OpenAPI specification handling

### Best Practices

#### CLI Design
- **Consistent Interface**: Follow standard CLI conventions
- **Helpful Errors**: Provide actionable error messages
- **Progressive Disclosure**: Simple defaults, advanced options
- **Documentation**: Comprehensive help and examples

#### Interactive Shell
- **User-Friendly**: Intuitive commands and feedback
- **Discoverable**: Tab completion and help system
- **Persistent**: Save history and context
- **Scriptable**: Support for automation

#### Benchmarking
- **Statistical Rigor**: Proper statistical analysis
- **Reproducible**: Consistent benchmark conditions
- **Meaningful Metrics**: Relevant performance indicators
- **Comparative**: Easy comparison across versions

### Usage Examples

#### Development Workflow
```bash
# Start development session
fda shell

# Run quick benchmark
> bench quick

# Validate API changes
> api validate --endpoint http://localhost:8080

# Generate test data
> adhoc generate_data --size 1000
```

#### Performance Testing
```bash
# Run full benchmark suite
fda bench --suite full --iterations 10

# Compare with baseline
fda bench --compare baseline.json

# Profile specific operations
fda bench --profile pipeline_creation
```

This crate serves as the development companion tool, providing essential utilities and automation for Feldera development workflows.
<!-- SECTION:crates/fda/CLAUDE.md END -->

---

## Context: crates/feldera-types/CLAUDE.md
<!-- SECTION:crates/feldera-types/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the feldera-types crate
cargo build -p feldera-types

# Run tests
cargo test -p feldera-types

# Check documentation
cargo doc -p feldera-types --open

# Run with all features
cargo build -p feldera-types --all-features
```

## Architecture Overview

### Technology Stack

- **Serialization**: serde with JSON support
- **Type System**: Rust's type system with trait-based abstractions
- **Configuration**: Structured configuration types
- **Error Handling**: Comprehensive error types and conversions

### Core Purpose

Feldera Types provides **shared type definitions** and **configuration structures** used across the entire Feldera platform:

- **Configuration Types**: Pipeline, connector, and transport configurations
- **Data Format Types**: Schema definitions for supported data formats
- **Error Types**: Standardized error handling across components
- **Transport Types**: Configuration for various transport mechanisms

### Project Structure

#### Core Modules

- `src/config.rs` - Pipeline and runtime configuration
- `src/transport/` - Transport-specific configuration types
- `src/format/` - Data format configuration and schemas
- `src/error.rs` - Error types and conversions
- `src/query.rs` - Query and program schema definitions

## Important Implementation Details

### Configuration Architecture

#### Pipeline Configuration
```rust
use feldera_types::config::PipelineConfig;

let config = PipelineConfig {
    workers: Some(4),
    storage: Some(storage_config),
    resources: Some(resource_limits),
    ..Default::default()
};
```

#### Transport Configuration
```rust
use feldera_types::transport::{KafkaInputConfig, HttpOutputConfig};

// Kafka input configuration
let kafka_config = KafkaInputConfig {
    brokers: vec!["localhost:9092".to_string()],
    topic: "input_topic".to_string(),
    group_id: Some("consumer_group".to_string()),
    ..Default::default()
};
```

### Data Format Types

#### Format Configuration
- **CSV**: Field delimiters, headers, escaping rules
- **JSON**: Schema validation, type coercion settings
- **Avro**: Schema registry integration, evolution policies
- **Parquet**: Compression, column selection, batch sizing

#### Schema Definitions
```rust
use feldera_types::program_schema::{Field, SqlType, Relation};

let table_schema = Relation {
    name: "users".to_string(),
    fields: vec![
        Field {
            name: "id".to_string(),
            columntype: SqlType::Integer { nullable: false },
            case_sensitive: false,
        },
        Field {
            name: "name".to_string(),
            columntype: SqlType::Varchar { nullable: true, precision: None },
            case_sensitive: false,
        },
    ],
    ..Default::default()
};
```

### Transport Types

#### Supported Transports
- **Kafka**: Broker configuration, topic settings, consumer groups
- **HTTP**: Endpoint URLs, authentication, rate limiting
- **File**: Path specifications, file formats, polling intervals
- **PostgreSQL**: Connection strings, table mappings, CDC settings
- **Delta Lake**: Storage locations, partition schemes, versioning

### Error Handling

#### Error Categories
- **Configuration Errors**: Invalid settings, missing required fields
- **Validation Errors**: Schema mismatches, type conflicts
- **Runtime Errors**: Transport failures, format conversion errors
- **System Errors**: Resource exhaustion, permission issues

```rust
use feldera_types::error::DetailedError;

// Structured error with context
let error = DetailedError::invalid_configuration(
    "Invalid Kafka broker configuration",
    Some("brokers field cannot be empty"),
);
```

## Development Workflow

### For New Configuration Types

1. Add configuration struct in appropriate module
2. Implement `Default`, `Serialize`, `Deserialize` traits
3. Add validation logic with `Validate` trait
4. Add comprehensive documentation with examples
5. Add unit tests for serialization/deserialization
6. Update dependent crates to use new configuration

### For New Data Types

1. Define type in appropriate module
2. Implement required traits (Clone, Debug, etc.)
3. Add serde support for JSON serialization
4. Add conversion methods to/from other representations
5. Add validation logic if needed
6. Test edge cases and error conditions

### Testing Strategy

#### Unit Tests
- Serialization round-trip testing
- Configuration validation testing
- Error message formatting
- Default value behavior

#### Integration Tests
- Cross-crate compatibility
- Real configuration file parsing
- Error propagation across boundaries

### Validation Framework

The crate includes a validation framework for configuration:

```rust
use feldera_types::config::Validate;

impl Validate for MyConfig {
    fn validate(&self) -> Result<(), DetailedError> {
        if self.workers == 0 {
            return Err(DetailedError::invalid_configuration(
                "workers must be greater than 0",
                None,
            ));
        }
        Ok(())
    }
}
```

### Serialization Patterns

#### JSON Serialization
- **Snake Case**: Field names use snake_case convention
- **Optional Fields**: Use `Option<T>` for optional configuration
- **Default Values**: Implement sensible defaults
- **Validation**: Validate after deserialization

#### Custom Serialization
```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", content = "config")]
pub enum TransportConfig {
    Kafka(KafkaConfig),
    Http(HttpConfig),
    File(FileConfig),
}
```

### Configuration Files

- `Cargo.toml` - Minimal dependencies for type definitions
- Feature flags for optional functionality
- Version compatibility across Feldera components

### Key Design Principles

- **Backward Compatibility**: Schema evolution without breaking changes
- **Type Safety**: Leverage Rust's type system for correctness
- **Documentation**: Comprehensive field documentation
- **Validation**: Runtime validation with helpful error messages
- **Modularity**: Separate concerns by transport/format type

### Dependencies

#### Core Dependencies
- `serde` - Serialization framework
- `serde_json` - JSON format support
- `chrono` - Date/time types
- `uuid` - UUID generation

#### Optional Dependencies
- `url` - URL parsing and validation
- `regex` - Pattern matching for validation
- `base64` - Encoding/decoding support

### Best Practices

#### Configuration Design
- **Sensible Defaults**: Most fields should have reasonable defaults
- **Clear Naming**: Field names should be self-documenting
- **Validation**: Validate configuration at construction time
- **Documentation**: Include examples in field documentation

#### Error Handling
- **Structured Errors**: Use DetailedError for rich error information
- **Context**: Provide helpful context in error messages
- **Recovery**: Design errors to be actionable
- **Consistency**: Use consistent error patterns across types

#### Type Design
- **Composability**: Types should compose well together
- **Extensibility**: Design for future extension
- **Performance**: Avoid unnecessary allocations
- **Testing**: Include comprehensive test coverage

### Usage Patterns

#### Configuration Loading
```rust
use feldera_types::config::PipelineConfig;

// Load from JSON file
let config: PipelineConfig = serde_json::from_str(&json_content)?;
config.validate()?;

// Merge with defaults
let final_config = PipelineConfig {
    workers: config.workers.or(Some(1)),
    ..config
};
```

#### Schema Validation
```rust
use feldera_types::program_schema::ProgramSchema;

// Validate program schema
let schema: ProgramSchema = serde_json::from_str(&schema_json)?;
schema.validate_consistency()?;
```

This crate is foundational to the Feldera platform, providing the type system backbone for configuration, data formats, and error handling across all components.
<!-- SECTION:crates/feldera-types/CLAUDE.md END -->

---

## Context: crates/fxp/CLAUDE.md
<!-- SECTION:crates/fxp/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the fxp crate
cargo build -p fxp

# Run tests
cargo test -p fxp

# Run with specific precision
cargo test -p fxp test_decimal_precision

# Check documentation
cargo doc -p fxp --open
```

## Architecture Overview

### Technology Stack

- **Fixed-Point Arithmetic**: High-precision decimal calculations
- **DBSP Integration**: Native integration with DBSP operators
- **Serialization**: rkyv and serde support
- **Performance**: Optimized arithmetic operations

### Core Purpose

FXP provides **high-precision fixed-point arithmetic** for financial and decimal computations:

- **Decimal Precision**: Exact decimal arithmetic without floating-point errors
- **DBSP Integration**: Native support for DBSP circuits
- **SQL Compatibility**: Match SQL DECIMAL semantics
- **Performance**: Optimized for high-throughput operations

### Project Structure

#### Core Modules

- `src/lib.rs` - Public API and core types
- `src/fixed.rs` - Fixed-point arithmetic implementation
- `src/u256.rs` - 256-bit integer arithmetic backend
- `src/dynamic.rs` - Dynamic precision support
- `src/dbsp_impl.rs` - DBSP integration
- `src/serde_impl.rs` - Serialization support
- `src/rkyv_impl.rs` - Zero-copy serialization

## Important Implementation Details

### Fixed-Point Types

#### Core Types
```rust
// Fixed-point decimal with compile-time precision
pub struct Fixed<const SCALE: i8> {
    value: I256,
}

// Dynamic precision decimal
pub struct DynamicFixed {
    value: I256,
    scale: i8,
    precision: u8,
}

// SQL DECIMAL type
pub type SqlDecimal = DynamicFixed;
```

#### Type Features
- **Compile-Time Scale**: Zero-cost fixed scale at compile time
- **Dynamic Scale**: Runtime configurable precision
- **Range**: Support for very large numbers (256-bit backend)
- **Exact Arithmetic**: No precision loss in calculations

### Arithmetic Operations

#### Core Operations
```rust
impl<const SCALE: i8> Fixed<SCALE> {
    pub fn add(self, other: Self) -> Self;
    pub fn sub(self, other: Self) -> Self;
    pub fn mul(self, other: Self) -> Self;
    pub fn div(self, other: Self) -> Option<Self>;

    // Scaling operations
    pub fn rescale<const NEW_SCALE: i8>(self) -> Fixed<NEW_SCALE>;
    pub fn round_to_scale<const NEW_SCALE: i8>(self) -> Fixed<NEW_SCALE>;
}
```

#### Advanced Operations
- **Rounding Modes**: Various rounding strategies (banker's rounding, etc.)
- **Scale Conversion**: Safe conversion between different scales
- **Overflow Handling**: Saturating or checked arithmetic
- **Comparison**: Total ordering with proper decimal semantics

### SQL Compatibility

#### SQL DECIMAL Semantics
```rust
// SQL DECIMAL(precision, scale)
pub fn sql_decimal(precision: u8, scale: i8) -> DynamicFixed {
    DynamicFixed::new(0, scale, precision)
}

// SQL arithmetic follows SQL standard rules
impl DynamicFixed {
    // Addition: max(scale1, scale2)
    pub fn sql_add(&self, other: &Self) -> Self;

    // Multiplication: scale1 + scale2
    pub fn sql_mul(&self, other: &Self) -> Self;

    // Division: configurable result scale
    pub fn sql_div(&self, other: &Self, result_scale: i8) -> Option<Self>;
}
```

#### SQL Standard Compliance
- **Precision Rules**: Follow SQL standard precision inference
- **Scale Rules**: Appropriate scale handling for all operations
- **Rounding**: SQL-compliant rounding behavior
- **Overflow**: Handle overflow according to SQL semantics

### DBSP Integration

#### DBSP Operator Support
```rust
use dbsp::operator::Fold;

impl<const SCALE: i8> Fold for Fixed<SCALE> {
    fn fold(&mut self, other: Self) {
        *self = self.add(other);
    }
}

// Aggregation support
impl Sum for Fixed<SCALE> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Fixed::zero(), |a, b| a.add(b))
    }
}
```

#### Zero-Copy Serialization
```rust
// rkyv support for zero-copy serialization
impl<const SCALE: i8> Archive for Fixed<SCALE> {
    type Archived = ArchivedFixed<SCALE>;
    type Resolver = ();

    fn resolve(&self, _: (), out: &mut Self::Archived);
}
```

### Performance Optimization

#### Arithmetic Optimization
- **Specialized Algorithms**: Optimized multiplication and division
- **Branch Prediction**: Minimize conditional branches
- **SIMD**: Vector operations where applicable
- **Memory Layout**: Optimal data structure layout

#### Scale Handling
- **Compile-Time Scale**: Zero runtime cost for fixed scales
- **Scale Caching**: Cache scale calculations
- **Batch Operations**: Optimize for batch processing
- **Precision Selection**: Choose optimal precision for operations

## Development Workflow

### For Arithmetic Extensions

1. Implement operation following SQL standard
2. Add appropriate overflow handling
3. Test with boundary conditions
4. Add performance benchmarks
5. Validate against SQL databases
6. Document precision and scale behavior

### For DBSP Integration

1. Implement required DBSP traits
2. Add serialization support
3. Test with DBSP circuits
4. Validate incremental behavior
5. Optimize for performance
6. Add comprehensive tests

### Testing Strategy

#### Arithmetic Testing
- **Precision**: Test precision preservation
- **Boundary Conditions**: Test with extreme values
- **SQL Compatibility**: Compare with SQL database results
- **Rounding**: Validate rounding behavior
- **Overflow**: Test overflow handling

#### DBSP Testing
- **Serialization**: Test rkyv round-trip
- **Incremental**: Test incremental computation
- **Aggregation**: Test aggregation operations
- **Performance**: Benchmark DBSP operations

### Precision Management

#### Scale Selection
```rust
// Choose appropriate scale for operations
pub fn optimal_scale_for_operation(
    op: ArithmeticOp,
    left_scale: i8,
    right_scale: i8,
) -> i8 {
    match op {
        ArithmeticOp::Add | ArithmeticOp::Sub => left_scale.max(right_scale),
        ArithmeticOp::Mul => left_scale + right_scale,
        ArithmeticOp::Div => left_scale - right_scale,
    }
}
```

#### Precision Control
- **Automatic Scaling**: Automatic scale selection for operations
- **Manual Control**: Explicit scale control when needed
- **Validation**: Validate precision requirements
- **Optimization**: Optimize precision for performance

### Configuration Files

- `Cargo.toml` - Fixed-point arithmetic dependencies
- Feature flags for different backends and optimizations

### Dependencies

#### Core Dependencies
- `num-bigint` - Large integer arithmetic
- `serde` - Serialization support
- `rkyv` - Zero-copy serialization

#### DBSP Dependencies
- `dbsp` - DBSP integration
- DBSP traits and operators

### Best Practices

#### Arithmetic Design
- **SQL Compliance**: Follow SQL standard precisely
- **Overflow Safety**: Handle overflow conditions safely
- **Performance**: Optimize critical arithmetic paths
- **Precision**: Maintain appropriate precision throughout

#### API Design
- **Type Safety**: Use compile-time scale where possible
- **Ergonomics**: Provide convenient conversion functions
- **Documentation**: Document precision and scale behavior
- **Testing**: Comprehensive test coverage

#### DBSP Integration
- **Zero-Copy**: Minimize data copying in serialization
- **Incremental**: Design for incremental computation
- **Aggregation**: Efficient aggregation operations
- **Memory**: Optimize memory usage patterns

### Usage Examples

#### Basic Arithmetic
```rust
use fxp::Fixed;

// Fixed scale at compile time
let a = Fixed::<2>::from_str("123.45").unwrap();  // 2 decimal places
let b = Fixed::<2>::from_str("67.89").unwrap();
let sum = a.add(b);  // 191.34

// Dynamic scale
let decimal = SqlDecimal::new(12345, 2, 10);  // 123.45 with precision 10, scale 2
```

#### SQL Operations
```rust
use fxp::DynamicFixed;

let price = DynamicFixed::from_sql_decimal(999, 2, 5);  // $9.99
let quantity = DynamicFixed::from_sql_decimal(3, 0, 3);  // 3
let total = price.sql_mul(&quantity);  // $29.97
```

This crate provides the mathematical foundation for precise decimal arithmetic in financial and scientific applications within the DBSP ecosystem.
<!-- SECTION:crates/fxp/CLAUDE.md END -->

---

## Context: crates/iceberg/CLAUDE.md
<!-- SECTION:crates/iceberg/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the iceberg crate
cargo build -p iceberg

# Run tests (requires test environment)
cargo test -p iceberg

# Set up test environment
cd src/test
python create_test_table_s3.py

# Install test dependencies
pip install -r requirements.txt
```

## Architecture Overview

### Technology Stack

- **Apache Iceberg**: Open table format for large analytic datasets
- **S3 Integration**: AWS S3 and S3-compatible storage
- **Async I/O**: Non-blocking I/O operations with tokio
- **Python Integration**: Test utilities with Python ecosystem

### Core Purpose

Iceberg provides **Apache Iceberg table format support** for Feldera:

- **Table Format**: Support for Iceberg's open table format
- **Cloud Storage**: Integration with S3 and cloud storage
- **Schema Evolution**: Handle schema changes over time
- **Time Travel**: Support for historical data queries

### Project Structure

#### Core Modules

- `src/lib.rs` - Core Iceberg functionality
- `src/input.rs` - Iceberg table input adapter
- `src/test/` - Test utilities and setup scripts

## Important Implementation Details

### Iceberg Integration

#### Table Format Support
```rust
pub struct IcebergTable {
    pub metadata: TableMetadata,
    pub schema: Schema,
    pub partition_spec: PartitionSpec,
}

impl IcebergTable {
    pub async fn load_from_catalog(&self, path: &str) -> Result<IcebergTable, IcebergError>;
    pub async fn scan(&self, filter: Option<Expression>) -> Result<FileScan, IcebergError>;
}
```

#### Features
- **Metadata Management**: Handle Iceberg metadata files
- **Schema Evolution**: Support schema changes over time
- **Partitioning**: Efficient data partitioning schemes
- **File Management**: Track data files and manifests

### Input Adapter

#### Data Ingestion
```rust
pub struct IcebergInputAdapter {
    table: IcebergTable,
    scan: FileScan,
    reader: ParquetReader,
}

impl InputAdapter for IcebergInputAdapter {
    async fn read_batch(&mut self) -> Result<RecordBatch, InputError> {
        let files = self.scan.next_files().await?;
        let batch = self.reader.read_files(files).await?;
        Ok(batch)
    }
}
```

#### Adapter Features
- **Incremental Reading**: Read only new/changed data
- **Parallel Processing**: Concurrent file reading
- **Format Support**: Parquet and other Iceberg-supported formats
- **Filter Pushdown**: Push filters to storage layer

### Cloud Storage Integration

#### S3 Support
```rust
pub struct S3IcebergCatalog {
    client: S3Client,
    warehouse_location: String,
}

impl IcebergCatalog for S3IcebergCatalog {
    async fn load_table(&self, name: &str) -> Result<IcebergTable, CatalogError>;
    async fn create_table(&self, name: &str, schema: Schema) -> Result<(), CatalogError>;
}
```

#### Storage Features
- **Multi-Cloud**: Support for AWS S3, Azure, GCS
- **Authentication**: Handle cloud credentials securely
- **Performance**: Optimized for cloud storage patterns
- **Cost Optimization**: Minimize storage operations

### Test Infrastructure

#### Python Test Setup
```python
# create_test_table_s3.py
import pyiceberg
from pyiceberg.catalog import load_catalog

def create_test_table():
    catalog = load_catalog("test")
    schema = pyiceberg.schema.Schema([
        pyiceberg.types.NestedField(1, "id", pyiceberg.types.LongType()),
        pyiceberg.types.NestedField(2, "name", pyiceberg.types.StringType()),
    ])

    catalog.create_table("test.table", schema)
```

#### Test Features
- **Realistic Data**: Generate realistic test datasets
- **Schema Variations**: Test various schema configurations
- **Performance Testing**: Measure ingestion performance
- **Integration Testing**: End-to-end pipeline testing

## Development Workflow

### For Iceberg Features

1. Study Apache Iceberg specification
2. Implement feature following Iceberg standards
3. Add comprehensive tests with real data
4. Test with various storage backends
5. Validate performance characteristics
6. Document compatibility and limitations

### For Storage Integration

1. Implement storage backend interface
2. Add authentication and configuration
3. Test with real cloud storage
4. Optimize for performance and cost
5. Add error handling and retry logic
6. Document setup and configuration

### Testing Strategy

#### Unit Tests
- **Metadata Parsing**: Test Iceberg metadata handling
- **Schema Evolution**: Test schema change scenarios
- **Partitioning**: Test partition pruning logic
- **Error Handling**: Test various error conditions

#### Integration Tests
- **Real Storage**: Test with actual S3 buckets
- **Large Data**: Test with realistic data sizes
- **Concurrent Access**: Test parallel reading
- **Schema Evolution**: Test with evolving schemas

### Configuration

#### Iceberg Configuration
```rust
pub struct IcebergConfig {
    pub catalog_type: CatalogType,
    pub warehouse_location: String,
    pub s3_endpoint: Option<String>,
    pub credentials: CredentialsConfig,
}

pub enum CatalogType {
    Hive,
    Hadoop,
    S3,
    Custom(Box<dyn IcebergCatalog>),
}
```

#### Storage Configuration
- **Credentials**: AWS credentials, IAM roles, access keys
- **Endpoints**: S3 endpoints, regions, custom endpoints
- **Performance**: Connection pooling, retry policies
- **Security**: Encryption, access control

### Performance Optimization

#### Read Optimization
- **Parallel Reading**: Read multiple files concurrently
- **Filter Pushdown**: Apply filters at storage level
- **Column Pruning**: Read only required columns
- **Vectorized Processing**: Efficient data processing

#### Memory Management
- **Streaming**: Stream large datasets without loading entirely
- **Buffer Management**: Optimize memory usage
- **Resource Control**: Limit concurrent operations
- **Garbage Collection**: Efficient memory cleanup

### Configuration Files

- `Cargo.toml` - Iceberg and cloud storage dependencies
- `src/test/requirements.txt` - Python test dependencies
- `src/test/requirements.ci.txt` - CI-specific Python dependencies

### Dependencies

#### Core Dependencies
- `iceberg-rs` - Rust Iceberg implementation
- `tokio` - Async runtime
- `aws-sdk-s3` - AWS S3 integration
- `parquet` - Parquet file format support

#### Test Dependencies
- `tempfile` - Temporary test files
- `uuid` - Test data generation
- Python ecosystem for test data setup

### Best Practices

#### Iceberg Usage
- **Standards Compliance**: Follow Apache Iceberg specification
- **Schema Design**: Design schemas for evolution
- **Partitioning**: Choose appropriate partitioning strategies
- **Metadata Management**: Handle metadata efficiently

#### Cloud Integration
- **Cost Awareness**: Minimize cloud storage costs
- **Performance**: Optimize for cloud storage patterns
- **Security**: Follow cloud security best practices
- **Reliability**: Handle transient cloud failures

#### Error Handling
- **Transient Errors**: Retry transient cloud failures
- **Schema Errors**: Handle schema incompatibilities gracefully
- **Resource Errors**: Handle resource exhaustion
- **Data Errors**: Handle corrupted or missing data

### Usage Examples

#### Basic Table Access
```rust
use iceberg::{IcebergTable, S3IcebergCatalog};

let catalog = S3IcebergCatalog::new(s3_config);
let table = catalog.load_table("warehouse.orders").await?;

let scan = table.scan(None).await?;
let batches = scan.collect().await?;
```

#### Filtered Reading
```rust
use iceberg::expressions::Expression;

let filter = Expression::gt("order_date", "2023-01-01");
let scan = table.scan(Some(filter)).await?;

for batch in scan {
    process_batch(batch?).await?;
}
```

This crate enables Feldera to work with modern data lake architectures using the Apache Iceberg table format for large-scale analytics.
<!-- SECTION:crates/iceberg/CLAUDE.md END -->

---

## Context: crates/ir/CLAUDE.md
<!-- SECTION:crates/ir/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the ir crate
cargo build -p ir

# Run tests
cargo test -p ir

# Regenerate test samples
./test/regen.bash

# Check documentation
cargo doc -p ir --open
```

## Architecture Overview

### Technology Stack

- **Compiler IR**: Multi-level intermediate representation
- **SQL Analysis**: SQL program analysis and transformation
- **Type System**: Rich type information and inference
- **Serialization**: JSON-based IR serialization

### Core Purpose

IR provides **intermediate representation layers** for SQL compilation:

- **HIR (High-level IR)**: Close to original SQL structure
- **MIR (Mid-level IR)**: Optimized and normalized representation
- **LIR (Low-level IR)**: Target-specific optimizations
- **Analysis**: Program analysis and transformation utilities

### Project Structure

#### Core Modules

- `src/hir.rs` - High-level intermediate representation
- `src/mir.rs` - Mid-level intermediate representation
- `src/lir.rs` - Low-level intermediate representation
- `src/lib.rs` - Common IR utilities and traits
- `test/` - Test samples and regeneration scripts

## Important Implementation Details

### IR Hierarchy

#### High-Level IR (HIR)
```rust
pub struct HirProgram {
    pub tables: Vec<TableDefinition>,
    pub views: Vec<ViewDefinition>,
    pub functions: Vec<FunctionDefinition>,
}

pub struct ViewDefinition {
    pub name: String,
    pub query: Query,
    pub schema: Schema,
}
```

HIR Features:
- **SQL-Close**: Maintains SQL structure and semantics
- **Type Information**: Rich type annotations
- **Metadata**: Preserves source location and comments
- **Validation**: Semantic validation and error reporting

#### Mid-Level IR (MIR)
```rust
pub struct MirProgram {
    pub operators: Vec<Operator>,
    pub data_flow: DataFlowGraph,
    pub optimizations: Vec<Optimization>,
}

pub enum Operator {
    Filter(FilterOp),
    Map(MapOp),
    Join(JoinOp),
    Aggregate(AggregateOp),
}
```

MIR Features:
- **Normalized**: Canonical operator representation
- **Optimized**: Applied optimization transformations
- **Data Flow**: Explicit data flow representation
- **Target Independent**: Platform-agnostic representation

#### Low-Level IR (LIR)
```rust
pub struct LirProgram {
    pub circuits: Vec<Circuit>,
    pub schedule: ExecutionSchedule,
    pub resources: ResourceRequirements,
}

pub struct Circuit {
    pub operators: Vec<PhysicalOperator>,
    pub connections: Vec<Connection>,
}
```

LIR Features:
- **Physical**: Target-specific operator selection
- **Scheduled**: Execution order and parallelization
- **Optimized**: Target-specific optimizations
- **Executable**: Ready for code generation

### Transformation Pipeline

#### HIR → MIR Transformation
```rust
pub struct HirToMirTransform {
    optimizer: Optimizer,
    normalizer: Normalizer,
}

impl HirToMirTransform {
    pub fn transform(&self, hir: HirProgram) -> Result<MirProgram, TransformError> {
        let normalized = self.normalizer.normalize(hir)?;
        let optimized = self.optimizer.optimize(normalized)?;
        Ok(optimized)
    }
}
```

Transformation Steps:
1. **Normalization**: Convert to canonical form
2. **Type Inference**: Infer missing type information
3. **Optimization**: Apply high-level optimizations
4. **Validation**: Ensure correctness preservation

#### MIR → LIR Transformation
```rust
pub struct MirToLirTransform {
    target: CompilationTarget,
    scheduler: Scheduler,
}

impl MirToLirTransform {
    pub fn transform(&self, mir: MirProgram) -> Result<LirProgram, TransformError> {
        let physical = self.select_operators(mir)?;
        let scheduled = self.scheduler.schedule(physical)?;
        Ok(scheduled)
    }
}
```

Transformation Steps:
1. **Operator Selection**: Choose physical operators
2. **Scheduling**: Determine execution order
3. **Resource Planning**: Allocate computational resources
4. **Code Generation**: Prepare for target code generation

### Analysis Framework

#### Program Analysis
```rust
pub trait ProgramAnalysis<IR> {
    type Result;
    type Error;

    fn analyze(&self, program: &IR) -> Result<Self::Result, Self::Error>;
}

// Data flow analysis
pub struct DataFlowAnalysis;
impl ProgramAnalysis<MirProgram> for DataFlowAnalysis {
    type Result = DataFlowInfo;
    type Error = AnalysisError;

    fn analyze(&self, program: &MirProgram) -> Result<DataFlowInfo, AnalysisError> {
        // Compute data flow information
    }
}
```

Analysis Types:
- **Type Analysis**: Type checking and inference
- **Data Flow**: Variable definitions and uses
- **Control Flow**: Program control structure
- **Dependency Analysis**: Operator dependencies

### Serialization Support

#### JSON Serialization
```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SerializableProgram {
    pub version: String,
    pub hir: Option<HirProgram>,
    pub mir: Option<MirProgram>,
    pub lir: Option<LirProgram>,
}
```

Serialization Features:
- **Version Control**: Track IR format versions
- **Partial Serialization**: Serialize individual IR levels
- **Human Readable**: JSON format for debugging
- **Round-Trip**: Preserve all information through serialization

## Development Workflow

### For IR Extensions

1. Define new IR nodes or transformations
2. Update serialization support
3. Add comprehensive tests
4. Update transformation passes
5. Regenerate test samples
6. Document changes and compatibility

### For Analysis Passes

1. Define analysis trait implementation
2. Add analysis-specific data structures
3. Implement analysis algorithm
4. Add validation and error handling
5. Test with various program patterns
6. Integrate with compilation pipeline

### Testing Strategy

#### Round-Trip Testing
- **Serialization**: Test JSON serialization round-trip
- **Transformation**: Test HIR→MIR→LIR transformations
- **Preservation**: Ensure semantic preservation
- **Error Handling**: Test error conditions

#### Sample Programs
```bash
# Regenerate test samples
cd test
./regen.bash

# Test with specific samples
cargo test test_sample_a
cargo test test_sample_b
```

### IR Validation

#### Semantic Validation
```rust
pub trait IrValidator<IR> {
    fn validate(&self, program: &IR) -> Vec<ValidationError>;
}

pub struct HirValidator;
impl IrValidator<HirProgram> for HirValidator {
    fn validate(&self, program: &HirProgram) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        // Check type consistency
        errors.extend(self.check_types(program));

        // Check name resolution
        errors.extend(self.check_names(program));

        errors
    }
}
```

#### Validation Categories
- **Type Safety**: Type consistency checking
- **Name Resolution**: Variable and function binding
- **Control Flow**: Reachability and termination
- **Resource Usage**: Memory and computation bounds

### Configuration Files

- `Cargo.toml` - IR processing dependencies
- `test/regen.bash` - Test sample regeneration script
- Sample files: `sample_*.sql` and `sample_*.json`

### Dependencies

#### Core Dependencies
- `serde` - Serialization framework
- `serde_json` - JSON support
- `thiserror` - Error handling

### Best Practices

#### IR Design
- **Immutable**: Design IR nodes as immutable
- **Type Rich**: Include comprehensive type information
- **Serializable**: Ensure all IR is serializable
- **Validated**: Include validation at each level

#### Transformation Design
- **Correctness**: Preserve program semantics
- **Composable**: Design transformations to compose
- **Reversible**: Consider round-trip transformations
- **Tested**: Comprehensive transformation testing

#### Analysis Design
- **Modular**: Design analyses to be composable
- **Incremental**: Support incremental analysis
- **Error Rich**: Provide detailed error information
- **Performance**: Optimize for large programs

### Usage Examples

#### Basic Transformation
```rust
use ir::{HirProgram, HirToMirTransform, MirToLirTransform};

// Load HIR from JSON
let hir: HirProgram = serde_json::from_str(&json_content)?;

// Transform through pipeline
let hir_to_mir = HirToMirTransform::new();
let mir = hir_to_mir.transform(hir)?;

let mir_to_lir = MirToLirTransform::new(target);
let lir = mir_to_lir.transform(mir)?;
```

#### Analysis Usage
```rust
use ir::{DataFlowAnalysis, ProgramAnalysis};

let analysis = DataFlowAnalysis::new();
let flow_info = analysis.analyze(&mir_program)?;

// Use analysis results for optimization
let optimizer = Optimizer::new(flow_info);
let optimized = optimizer.optimize(mir_program)?;
```

This crate provides the compiler infrastructure that enables sophisticated analysis and optimization of SQL programs during the compilation process.
<!-- SECTION:crates/ir/CLAUDE.md END -->

---

## Context: crates/nexmark/CLAUDE.md
<!-- SECTION:crates/nexmark/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the nexmark crate
cargo build -p nexmark

# Run data generation example
cargo run --example generate -p nexmark

# Run benchmarks
cargo bench -p nexmark

# Generate NEXMark data
cargo run -p nexmark --bin generate -- --events 1000000
```

## Architecture Overview

### Technology Stack

- **Benchmark Suite**: Industry-standard streaming benchmark
- **Data Generation**: Realistic auction data simulation
- **Query Implementation**: 22 standard benchmark queries
- **Performance Testing**: Throughput and latency measurement

### Core Purpose

NEXMark provides **streaming benchmark capabilities** for the DBSP engine:

- **Data Generation**: Realistic auction, bidder, and person data
- **Query Suite**: 22 standardized streaming queries
- **Performance Measurement**: Benchmark execution and metrics
- **Testing Framework**: Validate DBSP streaming performance

### Project Structure

#### Core Modules

- `src/generator/` - Data generation for auctions, bids, and people
- `src/queries/` - Implementation of all 22 NEXMark queries
- `src/model.rs` - Data model definitions
- `src/config.rs` - Benchmark configuration parameters

## Important Implementation Details

### Data Model

#### Core Entities
```rust
// Person entity (bidders and sellers)
pub struct Person {
    pub id: usize,
    pub name: String,
    pub email: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: u64,
}

// Auction entity
pub struct Auction {
    pub id: usize,
    pub seller: usize,
    pub category: usize,
    pub initial_bid: usize,
    pub date_time: u64,
    pub expires: u64,
}

// Bid entity
pub struct Bid {
    pub auction: usize,
    pub bidder: usize,
    pub price: usize,
    pub date_time: u64,
}
```

### Data Generation

#### Realistic Data Patterns
The generator produces realistic auction data:
- **Temporal Patterns**: Realistic time distributions
- **Price Models**: Market-based pricing behavior
- **Geographic Distribution**: Realistic location data
- **Correlation**: Realistic relationships between entities

#### Configuration Options
```rust
pub struct Config {
    pub events_per_second: usize,
    pub auction_proportion: f64,
    pub bid_proportion: f64,
    pub person_proportion: f64,
    pub num_categories: usize,
    pub auction_length_seconds: usize,
}
```

### Query Suite

#### Standard NEXMark Queries

**Q0: Pass Through**
- Simple data throughput measurement
- No computation, pure I/O benchmark

**Q1: Currency Conversion**
- Convert bid prices to different currency
- Tests map operations performance

**Q2: Selection**
- Filter auctions by category
- Tests filtering performance

**Q3: Local Item Suggestion**
- Join people and auctions by location
- Tests join performance

**Q4: Average Price for Category**
- Windowed aggregation over auction categories
- Tests windowing and aggregation

**Q5-Q22**: Complex streaming queries testing various aspects:
- Complex joins across multiple streams
- Windowed aggregations with various time bounds
- Pattern matching and sequence detection
- Multi-stage processing pipelines

### Benchmark Execution

#### Performance Metrics
```rust
pub struct BenchmarkResults {
    pub throughput_events_per_second: f64,
    pub latency_percentiles: LatencyDistribution,
    pub memory_usage: MemoryStats,
    pub cpu_utilization: f64,
}
```

#### Query Categories
- **Simple Queries (Q0-Q2)**: Basic operations
- **Join Queries (Q3, Q5, Q7-Q11)**: Multi-stream joins
- **Aggregation Queries (Q4, Q6, Q12-Q15)**: Windowed aggregates
- **Complex Queries (Q16-Q22)**: Advanced streaming patterns

## Development Workflow

### For Query Implementation

1. Study NEXMark specification for query semantics
2. Implement query using DBSP operators
3. Add comprehensive test cases with known results
4. Benchmark performance against reference implementations
5. Optimize for DBSP's incremental computation model
6. Validate correctness with various data distributions

### For Data Generation

1. Modify generator in `src/generator/`
2. Ensure realistic data distributions
3. Test with various configuration parameters
4. Validate data consistency and relationships
5. Benchmark generation performance
6. Test with different event rates and patterns

### Testing Strategy

#### Correctness Testing
- **Known Results**: Test queries with pre-computed results
- **Cross-Validation**: Compare with reference implementations
- **Edge Cases**: Empty streams, single events, boundary conditions
- **Data Consistency**: Validate generated data relationships

#### Performance Testing
- **Throughput**: Events processed per second
- **Latency**: End-to-end processing delay
- **Memory Usage**: Peak and steady-state memory
- **Scalability**: Performance across different data rates

### Configuration Options

#### Data Generation Config
```rust
let config = Config {
    events_per_second: 10_000,
    auction_proportion: 0.1,
    bid_proportion: 0.8,
    person_proportion: 0.1,
    num_categories: 100,
    auction_length_seconds: 600,
};
```

#### Benchmark Config
- **Event Rate**: Target events per second
- **Duration**: Benchmark runtime
- **Warmup**: Warmup period before measurement
- **Data Size**: Total events to generate

### Query Implementations

Each query demonstrates different DBSP capabilities:

#### Stream Processing Patterns
- **Filtering**: Select relevant events
- **Mapping**: Transform event data
- **Joining**: Correlate events across streams
- **Aggregating**: Compute statistics over windows
- **Windowing**: Time-based event grouping

#### Advanced Features
- **Watermarks**: Handle out-of-order events
- **Late Data**: Process delayed events
- **State Management**: Maintain query state
- **Result Updates**: Incremental result computation

### Configuration Files

- `Cargo.toml` - Benchmark and data generation dependencies
- Benchmark configuration in code (no external config files)
- Query-specific parameters in individual query modules

### Dependencies

#### Core Dependencies
- `rand` - Random data generation
- `chrono` - Date/time handling
- `serde` - Data serialization
- `csv` - Data export formats

#### DBSP Integration
- `dbsp` - Core streaming engine
- Benchmark harness integration
- Performance measurement tools

### Best Practices

#### Query Implementation
- **Incremental Friendly**: Design for incremental computation
- **Resource Aware**: Consider memory and CPU usage
- **Realistic**: Match real-world query patterns
- **Tested**: Comprehensive correctness validation

#### Data Generation
- **Realistic Distribution**: Match real auction behavior
- **Configurable**: Support various benchmark scenarios
- **Repeatable**: Deterministic generation for testing
- **Scalable**: Handle various event rates efficiently

#### Performance Measurement
- **Warm-up**: Allow system to reach steady state
- **Statistical Significance**: Multiple runs with confidence intervals
- **Resource Monitoring**: Track all relevant metrics
- **Reproducible**: Consistent measurement methodology

This crate provides industry-standard benchmarking for streaming systems, enabling performance validation and optimization of DBSP-based applications.
<!-- SECTION:crates/nexmark/CLAUDE.md END -->

---

## Context: crates/pipeline-manager/CLAUDE.md
<!-- SECTION:crates/pipeline-manager/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the pipeline manager
cargo build -p pipeline-manager

# Run tests
cargo test -p pipeline-manager

# Run integration tests
cargo test -p pipeline-manager --test integration_test

# Build with all features
cargo build -p pipeline-manager --all-features
```

### Running the Service

```bash
# Run the pipeline manager server
cargo run -p pipeline-manager

# Run with specific configuration
RUST_LOG=debug cargo run -p pipeline-manager -- --config config.toml

# Run database migrations
cargo run -p pipeline-manager -- --migrate-database

# Dump OpenAPI specification
cargo run -p pipeline-manager -- --dump-openapi
```

### Development Tools

```bash
# Run with hot reload using cargo-watch
cargo watch -x "run -p pipeline-manager"

# Check database connectivity
cargo run -p pipeline-manager -- --probe-db

# Generate banner
cargo run -p pipeline-manager -- --print-banner
```

## Architecture Overview

### Technology Stack

- **Web Framework**: Actix-web for HTTP server
- **Database**: PostgreSQL with SQLx for async database operations
- **Authentication**: JWT-based authentication with configurable providers
- **API Documentation**: OpenAPI/Swagger specification generation
- **Build Tools**: Custom build scripts for banner generation

### Service Components

- **API Server**: RESTful HTTP API for pipeline management
- **Database Layer**: PostgreSQL storage for pipelines, programs, and metadata
- **Compiler Integration**: SQL-to-DBSP and Rust compilation orchestration
- **Runner Service**: Pipeline execution and lifecycle management
- **Authentication**: Multi-provider authentication system

### Project Structure

#### Core Directories

- `src/api/` - HTTP API endpoints and request handling
- `src/db/` - Database operations and schema management
- `src/compiler/` - SQL and Rust compilation integration
- `src/runner/` - Pipeline execution and management
- `src/auth.rs` - Authentication and authorization
- `migrations/` - Database migration scripts

#### Key Components

- **API Endpoints**: CRUD operations for pipelines and programs
- **Database Abstraction**: Type-safe database operations
- **Compiler Services**: Integration with SQL-to-DBSP compiler
- **Pipeline Execution**: Runtime management and monitoring

## Important Implementation Details

### Database Schema

The service uses PostgreSQL with versioned migrations:

- **Programs**: SQL program definitions and compilation status
- **Pipelines**: Pipeline configurations and runtime state
- **API Keys**: Authentication credentials and permissions
- **Tenants**: Multi-tenancy support

### API Structure

#### Core Endpoints

```
GET    /api/programs           - List programs
POST   /api/programs           - Create program
GET    /api/programs/{id}      - Get program details
PATCH  /api/programs/{id}      - Update program
DELETE /api/programs/{id}      - Delete program

GET    /api/pipelines          - List pipelines
POST   /api/pipelines          - Create pipeline
GET    /api/pipelines/{id}     - Get pipeline details
PATCH  /api/pipelines/{id}     - Update pipeline
DELETE /api/pipelines/{id}     - Delete pipeline

GET    /v0/config              - Get configuration (includes tenant info)
GET    /config/authentication  - Get authentication provider configuration
GET    /config/demos           - Get list of available demos
```

#### Pipeline Lifecycle

```
POST /api/pipelines/{id}/start    - Start pipeline
POST /api/pipelines/{id}/pause    - Pause pipeline
POST /api/pipelines/{id}/shutdown - Stop pipeline
GET  /api/pipelines/{id}/stats    - Get runtime statistics
```

### Compilation Pipeline

1. **SQL Parsing**: Validate SQL program syntax
2. **DBSP Generation**: Convert SQL to DBSP circuit
3. **Rust Compilation**: Compile generated Rust code
4. **Binary Packaging**: Create executable pipeline binary
5. **Deployment**: Deploy and manage pipeline execution

### Authentication System

The pipeline-manager supports multiple authentication providers through a unified OIDC/OAuth2 framework:

#### **Supported Providers**
- **None**: No authentication (development/testing)
- **AWS Cognito**
- **Generic OIDC** - Okta

#### **Authentication Mechanisms**
- **OIDC Tokens**: OIDC-compliant Access token validation with RS256 signature verification
- **API Keys**: User-generated keys that

Authentication of HTTP API requests is performed through Authorization header via `Bearer <token>` value

Additional features:
- JWK Caching: Automatic public key fetching and caching from provider endpoints
- Bearer Token Authorization: Client sends Access OIDC token as Bearer token; all claims (tenant, groups, etc.) are extracted from this Access token

#### **Configuration**
```bash
# Environment variables for OIDC providers
AUTH_ISSUER=https://your-domain.okta.com/oauth2/<custom-auth-server-id>
AUTH_CLIENT_ID=your-client-id

# For AWS Cognito (additional variables)
AWS_COGNITO_LOGIN_URL=https://your-domain.auth.region.amazoncognito.com/login
AWS_COGNITO_LOGOUT_URL=https://your-domain.auth.region.amazoncognito.com/logout
```

#### **Authorization mechanisms**

The authentication system supports flexible tenant assignment strategies across all supported OIDC providers (AWS Cognito, Okta):

**Tenant Assignment Strategies:**
- `--individual-tenant` (default: true) - Creates individual tenants based on user's `sub` claim
- `--issuer-tenant` (default: false) - Derives tenant name from auth issuer hostname (e.g., `company.okta.com`)
- Custom `tenant` claim - enabled by default - If present in OIDC Access token is used to directly determine the authorized tenant

**Tenant Resolution Priority (all providers):**
1. `tenant` claim (explicit tenant assignment via OIDC provider)
2. Issuer domain extraction (when `--issuer-tenant` enabled)
3. User `sub` claim (when `--individual-tenant` enabled)

**Group-based authorization**
If --authorized_groups is configured the user has to have at least one of these groups in `groups` claim of OIDC Access Token

#### **Enterprise Features**
- **Fault tolerance**: Mechanism to recover from a crash by making periodic checkpoints, identifying and replaying lost state

## Development Workflow

### For API Changes

1. Modify endpoint handlers in `src/api/endpoints/`
2. Update database operations in `src/db/operations/`
3. Add/update database migrations in `migrations/`
4. Update OpenAPI specification
5. Add integration tests

### For Database Changes

1. Create migration in `migrations/V{n}__{description}.sql`
2. Update corresponding types in `src/db/types/`
3. Modify database operations in `src/db/operations/`
4. Test migration rollback scenarios
5. Update integration tests

### Testing Strategy

#### Unit Tests
- Database operation testing with test fixtures
- API endpoint testing with mock dependencies
- Authentication and authorization testing

#### Integration Tests
- End-to-end API testing with real database
- Pipeline lifecycle testing
- Multi-tenant isolation verification

### Configuration Management

The service supports multiple configuration sources:

- **Environment Variables**: Runtime configuration
- **Configuration Files**: TOML-based configuration
- **Command Line Arguments**: Override configuration
- **Database Configuration**: Dynamic configuration storage

### Error Handling

- **Structured Errors**: Type-safe error propagation
- **HTTP Error Mapping**: Appropriate HTTP status codes
- **Database Error Handling**: Transaction rollback and recovery
- **Compilation Error Reporting**: Detailed error messages

### Configuration Files

- `Cargo.toml` - Package configuration with database features
- `build.rs` - Build-time banner and asset generation
- `migrations/` - Database schema evolution
- `openapi.json` - Generated API specification

### Key Features

- **Multi-tenancy**: Isolated environments per tenant
- **High Availability**: Database connection pooling and retry logic
- **Monitoring**: Structured logging and metrics
- **Security**: Input validation and SQL injection prevention

### Dependencies

#### Core Dependencies
- `actix-web` - HTTP server framework
- `sqlx` - Async PostgreSQL client
- `serde` - Serialization/deserialization
- `tokio` - Async runtime

#### Database Dependencies
- `sqlx-postgres` - PostgreSQL driver
- `uuid` - UUID generation
- `chrono` - Date/time handling

#### Authentication Dependencies
- `jsonwebtoken` - JWT token handling
- `argon2` - Password hashing
- `oauth2` - OAuth2 client support

### Performance Considerations

- **Connection Pooling**: Database connection management
- **Async Processing**: Non-blocking I/O operations
- **Caching**: In-memory caching of frequently accessed data
- **Batch Operations**: Efficient bulk database operations

### Security Best Practices

- **Input Validation**: Comprehensive request validation
- **SQL Injection Prevention**: Parameterized queries
- **Authentication**: Secure token management
- **Authorization**: Fine-grained access control
- **Audit Logging**: Security event tracking

### Development Tools Integration

- **Hot Reload**: Development server with automatic restart
- **Database Migrations**: Version-controlled schema changes
- **OpenAPI Generation**: Automatic API documentation
- **Banner Generation**: Build-time asset creation

### Monitoring and Observability

- **Structured Logging**: JSON-formatted log output
- **Metrics Collection**: Performance and usage metrics
- **Health Checks**: Service health monitoring endpoints
- **Distributed Tracing**: Request tracing across services
<!-- SECTION:crates/pipeline-manager/CLAUDE.md END -->

---

## Context: crates/rest-api/CLAUDE.md
<!-- SECTION:crates/rest-api/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the rest-api crate
cargo build -p rest-api

# Generate OpenAPI specification
cargo run -p rest-api --bin generate_openapi

# Validate OpenAPI spec
cargo test -p rest-api
```

## Architecture Overview

### Technology Stack

- **OpenAPI Generation**: Automatic API specification generation
- **Type Definitions**: Shared types for REST API
- **Build Integration**: Build-time API specification generation

### Core Purpose

REST API provides **API specification and type definitions**:

- **OpenAPI Spec**: Machine-readable API specification
- **Type Safety**: Shared types between server and client
- **Documentation**: API documentation generation
- **Code Generation**: Support for client SDK generation

### Project Structure

#### Core Files

- `src/lib.rs` - Core API type definitions
- `build.rs` - Build-time OpenAPI generation
- `openapi.json` - Generated OpenAPI specification

## Important Implementation Details

### OpenAPI Specification

#### Generated Specification
The crate generates a complete OpenAPI 3.0 specification including:
- **Endpoints**: All REST API endpoints
- **Schemas**: Request/response data structures
- **Authentication**: Security scheme definitions
- **Examples**: Request/response examples

#### Specification Features
```json
{
  "openapi": "3.0.0",
  "info": {
    "title": "Feldera API",
    "version": "0.115.0"
  },
  "paths": {
    "/api/pipelines": {
      "get": {
        "summary": "List pipelines",
        "responses": { ... }
      }
    }
  },
  "components": {
    "schemas": { ... }
  }
}
```

### Type Definitions

#### Core API Types
```rust
// Pipeline management types
pub struct Pipeline {
    pub id: PipelineId,
    pub name: String,
    pub description: Option<String>,
    pub config: PipelineConfig,
    pub status: PipelineStatus,
}

// Program management types
pub struct Program {
    pub id: ProgramId,
    pub name: String,
    pub code: String,
    pub schema: Option<ProgramSchema>,
}
```

#### Type Features
- **Serialization**: Full serde support for JSON
- **Validation**: Input validation with detailed errors
- **Documentation**: Comprehensive field documentation
- **Compatibility**: Version compatibility across releases

### Build-Time Generation

#### Build Script Integration
```rust
// build.rs
fn main() {
    generate_openapi_spec();
    validate_spec_completeness();
    update_client_bindings();
}
```

#### Generation Process
1. **Extract Types**: Analyze Rust type definitions
2. **Generate Schemas**: Convert to OpenAPI schemas
3. **Validate Spec**: Ensure specification completeness
4. **Write Output**: Generate `openapi.json` file

## Development Workflow

### For API Changes

1. Update type definitions in `src/lib.rs`
2. Add appropriate serde annotations
3. Add documentation comments
4. Run build to regenerate OpenAPI spec
5. Validate specification completeness
6. Update client SDKs if needed

### For New Endpoints

1. Define request/response types
2. Add appropriate validation
3. Document all fields and examples
4. Ensure consistent naming patterns
5. Test serialization/deserialization
6. Update API documentation

### Testing Strategy

#### Type Testing
- **Serialization**: Round-trip JSON serialization
- **Validation**: Input validation edge cases
- **Schema**: OpenAPI schema compliance
- **Compatibility**: Backward compatibility testing

#### Specification Testing
- **Completeness**: All endpoints documented
- **Validity**: Valid OpenAPI 3.0 specification
- **Examples**: All examples are valid
- **Consistency**: Consistent naming and patterns

### OpenAPI Features

#### Schema Generation
- **Automatic**: Generated from Rust types
- **Comprehensive**: Complete type information
- **Validated**: Ensures spec correctness
- **Examples**: Includes realistic examples

#### Documentation Integration
- **Interactive**: Swagger UI integration
- **Searchable**: Full-text search support
- **Versioned**: Version-specific documentation
- **Client Generation**: Support for multiple languages

### Configuration Files

- `Cargo.toml` - Minimal dependencies for type definitions
- `build.rs` - OpenAPI generation logic
- `openapi.json` - Generated API specification

### Dependencies

#### Core Dependencies
- `serde` - Serialization/deserialization
- `serde_json` - JSON support
- `uuid` - UUID type support
- `chrono` - Date/time types

#### Build Dependencies
- `openapi` - OpenAPI specification generation
- `schemars` - JSON schema generation
- `utoipa` - OpenAPI derive macros

### Best Practices

#### Type Design
- **Consistency**: Follow consistent naming patterns
- **Documentation**: Document all public types and fields
- **Validation**: Include appropriate validation rules
- **Examples**: Provide realistic examples

#### API Design
- **RESTful**: Follow REST principles
- **Consistent**: Consistent response formats
- **Versioned**: Support for API versioning
- **Error Handling**: Structured error responses

#### Documentation
- **Comprehensive**: Document all endpoints and types
- **Examples**: Include request/response examples
- **Error Cases**: Document error conditions
- **Changelog**: Track API changes across versions

### Usage Examples

#### Type Usage
```rust
use rest_api::{Pipeline, PipelineStatus};

let pipeline = Pipeline {
    id: PipelineId::new(),
    name: "my-pipeline".to_string(),
    status: PipelineStatus::Running,
    ..Default::default()
};

let json = serde_json::to_string(&pipeline)?;
```

#### OpenAPI Integration
```bash
# Generate client SDK from spec
openapi-generator generate \
    -i openapi.json \
    -g typescript-fetch \
    -o client/typescript
```

This crate provides the foundational API types and specifications that enable consistent API usage across all Feldera components and client SDKs.
<!-- SECTION:crates/rest-api/CLAUDE.md END -->

---

## Context: crates/sqllib/CLAUDE.md
<!-- SECTION:crates/sqllib/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the sqllib crate
cargo build -p sqllib

# Run tests
cargo test -p sqllib

# Run specific test modules
cargo test -p sqllib test_decimal
cargo test -p sqllib test_string_functions

# Check documentation
cargo doc -p sqllib --open
```

## Architecture Overview

### Technology Stack

- **Numeric Computing**: High-precision decimal arithmetic
- **String Processing**: Unicode-aware string operations
- **Date/Time**: Comprehensive temporal data support
- **Spatial Data**: Geographic point operations
- **Type System**: Rich SQL type mapping to Rust types

### Core Purpose

SQL Library provides **runtime support functions** for SQL operations compiled by the SQL-to-DBSP compiler:

- **Aggregate Functions**: SUM, COUNT, AVG, MIN, MAX, etc.
- **Scalar Functions**: String manipulation, date/time operations, mathematical functions
- **Type System**: SQL type representations with null handling
- **Operators**: Arithmetic, comparison, logical operations
- **Casting**: Type conversions between SQL types

### Project Structure

#### Core Modules

- `src/aggregates.rs` - SQL aggregate function implementations
- `src/operators.rs` - Arithmetic and comparison operators
- `src/casts.rs` - Type conversion functions
- `src/string.rs` - String manipulation functions
- `src/timestamp.rs` - Date and time operations
- `src/decimal.rs` - High-precision decimal arithmetic
- `src/array.rs` - SQL array operations
- `src/map.rs` - SQL map (key-value) operations

## Important Implementation Details

### Type System

#### Core SQL Types
```rust
// Nullable wrapper for all SQL types
pub type SqlOption<T> = Option<T>;

// String types with proper null handling
pub type SqlString = Option<String>;
pub type SqlChar = Option<String>;

// Numeric types
pub type SqlInt = Option<i32>;
pub type SqlBigInt = Option<i64>;
pub type SqlDecimal = Option<Decimal>;

// Temporal types
pub type SqlTimestamp = Option<Timestamp>;
pub type SqlDate = Option<Date>;
pub type SqlTime = Option<Time>;
```

#### Null Handling
All SQL operations properly handle NULL values according to SQL semantics:
```rust
// NULL propagation in arithmetic
pub fn add_int(left: SqlInt, right: SqlInt) -> SqlInt {
    match (left, right) {
        (Some(l), Some(r)) => Some(l.saturating_add(r)),
        _ => None, // NULL if either operand is NULL
    }
}
```

### Aggregate Functions

#### Standard Aggregates
```rust
use sqllib::aggregates::*;

// SUM with proper null handling
let result = sum_int(values);

// COUNT (never returns null)
let count = count_star(values);

// AVG with decimal precision
let average = avg_decimal(decimal_values);
```

#### Custom Aggregates
- **Streaming Aggregation**: Incremental computation support
- **Window Functions**: OVER clause support
- **Distinct Aggregation**: COUNT(DISTINCT ...) support
- **Conditional Aggregation**: Filtered aggregates

### String Functions

#### Unicode-Aware Operations
```rust
use sqllib::string::*;

// Case conversion with Unicode support
let upper = upper_string(input);
let lower = lower_string(input);

// Substring operations
let substr = substring(string, start, length);

// Pattern matching
let matches = like_string(text, pattern);
```

#### String Interning
For performance optimization, the library includes string interning:
```rust
use sqllib::string_interner::StringInterner;

let mut interner = StringInterner::new();
let interned = interner.intern("repeated_string");
```

### Date/Time Functions

#### Timestamp Operations
```rust
use sqllib::timestamp::*;

// Date arithmetic
let future = add_interval_to_timestamp(timestamp, interval);

// Date extraction
let year = extract_year_from_timestamp(timestamp);
let month = extract_month_from_timestamp(timestamp);

// Formatting
let formatted = format_timestamp(timestamp, format_string);
```

#### Interval Support
- **Interval Arithmetic**: Addition/subtraction with dates
- **Duration Calculations**: Time differences
- **Timezone Handling**: UTC and local time support

### Decimal Arithmetic

#### High-Precision Decimals
```rust
use sqllib::decimal::*;

// Precise decimal operations
let result = add_decimal(decimal1, decimal2);
let division = div_decimal(dividend, divisor);

// Scale and precision handling
let rounded = round_decimal(value, precision);
```

### Array and Map Operations

#### SQL Arrays
```rust
use sqllib::array::*;

// Array construction
let array = array_constructor(elements);

// Array access
let element = array_element(array, index);

// Array aggregation
let concatenated = array_concat(array1, array2);
```

#### SQL Maps
```rust
use sqllib::map::*;

// Map construction
let map = map_constructor(keys, values);

// Map access
let value = map_element(map, key);
```

## Development Workflow

### For New SQL Functions

1. Add function implementation in appropriate module
2. Follow SQL standard semantics for null handling
3. Add comprehensive unit tests with edge cases
4. Update function catalog if needed for SQL compiler
5. Add performance benchmarks for critical functions
6. Document SQL compatibility and behavior

### For Type Extensions

1. Add type definition with proper null handling
2. Implement required traits (Clone, Debug, PartialEq, etc.)
3. Add conversion functions to/from other types
4. Add serialization support if needed
5. Test boundary conditions and error cases

### Testing Strategy

#### Unit Tests
- **Null Handling**: Test NULL propagation in all functions
- **Edge Cases**: Boundary values, overflow conditions
- **SQL Compatibility**: Match SQL standard behavior
- **Error Conditions**: Invalid inputs, division by zero

#### Property Tests
- **Arithmetic Properties**: Commutativity, associativity
- **String Properties**: Length preservation, encoding
- **Aggregate Properties**: Incremental vs batch computation

### Performance Optimization

#### Critical Path Optimization
- **Hot Functions**: Optimize frequently called functions
- **Memory Allocation**: Minimize allocations in tight loops
- **SIMD**: Vector operations for batch processing
- **Caching**: Cache expensive computations

#### Null Handling Optimization
```rust
// Efficient null checking patterns
match (left, right) {
    (Some(l), Some(r)) => Some(compute(l, r)),
    _ => None,
}

// Avoiding unnecessary allocations
pub fn string_function(input: &SqlString) -> SqlString {
    input.as_ref().map(|s| process_string(s))
}
```

### Error Handling

#### SQL Error Semantics
- **Division by Zero**: Return NULL, not panic
- **Overflow**: Saturating arithmetic or NULL
- **Invalid Dates**: Return NULL for invalid date constructions
- **Type Errors**: Compile-time prevention where possible

### Configuration Files

- `Cargo.toml` - Dependencies for numeric, temporal, and string processing
- Feature flags for optional functionality (geo, uuid, etc.)

### Key Features

- **SQL Compatibility**: Match standard SQL behavior
- **Null Safety**: Comprehensive NULL value handling
- **Performance**: Optimized for DBSP's incremental computation
- **Unicode Support**: Proper Unicode string handling
- **Precision**: High-precision decimal arithmetic

### Dependencies

#### Core Dependencies
- `rust_decimal` - High-precision decimal arithmetic
- `chrono` - Date and time manipulation
- `uuid` - UUID type support
- `geo` - Geographic data types

#### String Dependencies
- `regex` - Pattern matching
- `unicode-normalization` - Unicode text processing
- `string-interner` - String interning for performance

### Best Practices

#### Function Implementation
- **Null First**: Always handle NULL values first
- **SQL Semantics**: Follow SQL standard behavior precisely
- **Performance**: Consider incremental computation patterns
- **Testing**: Test with representative data distributions

#### Memory Management
- **Clone Minimization**: Use references where possible
- **String Handling**: Avoid unnecessary string allocations
- **Decimal Precision**: Use appropriate precision for operations
- **Array Operations**: Efficient vector operations

#### Error Design
- **No Panics**: Functions should never panic on valid input
- **Graceful Degradation**: Return NULL for invalid operations
- **Type Safety**: Leverage Rust's type system for correctness

This crate is essential for SQL compatibility, providing the runtime functions that make SQL operations work correctly within the DBSP computational model.
<!-- SECTION:crates/sqllib/CLAUDE.md END -->

---

## Context: crates/storage/CLAUDE.md
<!-- SECTION:crates/storage/CLAUDE.md START -->
## Overview

## Key Development Commands

### Building and Testing

```bash
# Build the storage crate
cargo build -p storage

# Run tests
cargo test -p storage

# Run tests with specific backend
cargo test -p storage --features=test_posixio
cargo test -p storage --features=test_memory

# Benchmark storage operations
cargo bench -p storage
```

## Architecture Overview

### Technology Stack

- **Storage Backends**: Pluggable storage implementations
- **Async I/O**: Non-blocking file operations with tokio
- **Error Handling**: Comprehensive storage error types
- **Testing**: Mock and real backend testing

### Core Purpose

Storage provides **persistent storage abstractions** for DBSP:

- **Backend Abstraction**: Pluggable storage implementations
- **Async Interface**: Non-blocking storage operations
- **Error Recovery**: Robust error handling and retry logic
- **Performance**: Optimized for DBSP's access patterns

### Project Structure

#### Core Modules

- `src/backend/` - Storage backend implementations
- `src/file.rs` - File-based storage interface
- `src/block.rs` - Block-level storage operations
- `src/fbuf.rs` - File buffer management
- `src/error.rs` - Storage error types

## Important Implementation Details

### Storage Backend Trait

#### Core Interface
```rust
pub trait StorageBackend: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn read(&self, path: &str, offset: u64, length: usize) -> Result<Vec<u8>, Self::Error>;
    async fn write(&self, path: &str, offset: u64, data: &[u8]) -> Result<(), Self::Error>;
    async fn delete(&self, path: &str) -> Result<(), Self::Error>;
    async fn exists(&self, path: &str) -> Result<bool, Self::Error>;
}
```

#### Backend Implementations

**Memory Backend**
- In-memory storage for testing
- Fast access, no persistence
- Used for unit tests and development

**POSIX I/O Backend**
- Standard filesystem operations
- Production storage backend
- Supports all POSIX-compliant filesystems

### File Interface

#### Async File Operations
```rust
pub struct AsyncFile {
    backend: Arc<dyn StorageBackend>,
    path: String,
}

impl AsyncFile {
    pub async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, StorageError>;
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> Result<(), StorageError>;
    pub async fn sync(&self) -> Result<(), StorageError>;
}
```

#### File Features
- **Random Access**: Read/write at arbitrary offsets
- **Async Operations**: Non-blocking I/O
- **Error Handling**: Robust error recovery
- **Sync Operations**: Force data to storage

### Block-Level Operations

#### Block Interface
```rust
pub struct Block {
    data: Vec<u8>,
    offset: u64,
    dirty: bool,
}

impl Block {
    pub fn read(&self, offset: usize, length: usize) -> &[u8];
    pub fn write(&mut self, offset: usize, data: &[u8]);
    pub fn is_dirty(&self) -> bool;
}
```

#### Block Features
- **Caching**: In-memory block caching
- **Dirty Tracking**: Optimize write operations
- **Batch Operations**: Efficient bulk I/O
- **Alignment**: Optimal block alignment

### File Buffer Management

#### Buffer Pool
```rust
pub struct FileBuffer {
    blocks: HashMap<u64, Block>,
    capacity: usize,
    backend: Arc<dyn StorageBackend>,
}

impl FileBuffer {
    pub async fn read_block(&mut self, offset: u64) -> Result<&Block, StorageError>;
    pub async fn write_block(&mut self, offset: u64, data: &[u8]) -> Result<(), StorageError>;
    pub async fn flush(&mut self) -> Result<(), StorageError>;
}
```

#### Buffer Features
- **LRU Eviction**: Least Recently Used block eviction
- **Write Coalescing**: Batch multiple writes
- **Read Ahead**: Predictive block loading
- **Memory Limits**: Bounded memory usage

## Development Workflow

### For New Storage Backend

1. Implement `StorageBackend` trait
2. Define backend-specific error types
3. Add configuration for backend parameters
4. Implement all required async methods
5. Add comprehensive error handling
6. Test with various failure scenarios
7. Add performance benchmarks

### For Storage Optimization

1. Profile existing storage patterns
2. Identify bottlenecks in I/O operations
3. Implement caching or batching optimizations
4. Test performance improvements
5. Validate correctness with stress tests
6. Document performance characteristics

### Testing Strategy

#### Unit Tests
- **Backend Interface**: Test all backend implementations
- **Error Conditions**: Network failures, disk full, permission errors
- **Concurrency**: Multiple simultaneous operations
- **Data Integrity**: Verify data consistency

#### Integration Tests
- **Real Storage**: Test with actual filesystems
- **Performance**: Throughput and latency measurement
- **Reliability**: Long-running stability tests
- **Recovery**: Error recovery and retry logic

### Error Handling

#### Error Categories
```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),

    #[error("Configuration error: {0}")]
    Config(String),
}
```

#### Error Handling Patterns
- **Retry Logic**: Automatic retry with exponential backoff
- **Circuit Breaker**: Fail fast when backend is unavailable
- **Graceful Degradation**: Continue with reduced functionality
- **Error Logging**: Structured error logging for debugging

### Performance Optimization

#### I/O Patterns
- **Sequential Access**: Optimize for sequential reads/writes
- **Batch Operations**: Group multiple operations
- **Alignment**: Align I/O to block boundaries
- **Prefetching**: Read-ahead for predictable patterns

#### Memory Management
- **Buffer Pooling**: Reuse memory buffers
- **Zero-Copy**: Minimize data copying
- **Compression**: Optional data compression
- **Memory Mapping**: Use mmap for large files

### Configuration

#### Backend Configuration
```rust
pub struct StorageConfig {
    pub backend: BackendType,
    pub cache_size: usize,
    pub block_size: usize,
    pub max_concurrent_ops: usize,
}
```

#### Tuning Parameters
- **Cache Size**: Memory allocated for caching
- **Block Size**: I/O operation granularity
- **Concurrency**: Maximum parallel operations
- **Retry Policy**: Error retry configuration

### Configuration Files

- `Cargo.toml` - Storage backend dependencies and feature flags
- Feature flags for different backends and testing modes

### Dependencies

#### Core Dependencies
- `tokio` - Async runtime and I/O
- `thiserror` - Error handling
- `futures` - Async utilities
- `tracing` - Structured logging

#### Backend Dependencies
- `libc` - POSIX I/O operations
- `memmap2` - Memory mapping
- `lz4` - Optional compression

### Best Practices

#### API Design
- **Async First**: All operations are async by default
- **Error Rich**: Detailed error information with context
- **Resource Safe**: Proper cleanup and resource management
- **Performance Aware**: Design for high-performance patterns

#### Implementation
- **Zero-Copy**: Minimize data copying where possible
- **Batch Operations**: Group operations for efficiency
- **Error Recovery**: Robust error handling and retry logic
- **Memory Bounds**: Prevent unbounded memory usage

#### Testing
- **Real Backends**: Test with actual storage systems
- **Failure Injection**: Test error conditions systematically
- **Performance Tests**: Validate performance characteristics
- **Concurrency Tests**: Test thread safety and race conditions

This crate provides the storage foundation that enables DBSP to persist state and handle large datasets that don't fit in memory.
<!-- SECTION:crates/storage/CLAUDE.md END -->

---

## Context: deploy/CLAUDE.md
<!-- SECTION:deploy/CLAUDE.md START -->
## Overview

The `deploy/` directory contains Docker configurations and deployment orchestration for the Feldera platform. These files support multiple deployment scenarios from development builds to production releases, and integrate with the GitHub Actions workflows to provide a comprehensive CI/CD and deployment pipeline.

## Dockerfile Architecture

### **Production Runtime Image** (`Dockerfile`)

**Purpose**: Creates the production runtime container that end users download and execute.

**Release Process Role**:
- Built and published by the `build-docker.yml` workflow during releases
- Contains the pipeline manager binary and SQL compiler runtime environment
- Tagged with version numbers (e.g., `ghcr.io/feldera/pipeline-manager:v0.115.0`) by `ci-release.yml`
- Serves as the base image for customer deployments and cloud installations

**Key Characteristics**:
```dockerfile
# Multi-stage build optimized for runtime
FROM ubuntu:24.04 AS base
# Runtime dependencies: OpenJDK 21, OpenSSL, protobuf-compiler
# Optimized for production with minimal attack surface
# Supports both AMD64 and ARM64 architectures
```

**Integration Points**:
- Used in production docker-compose configurations
- Published to GitHub Container Registry by release workflows
- Referenced in quickstart documentation and deployment guides
- Base for customer self-hosted deployments

### **CI Development Environment** (`build.Dockerfile`)

**Purpose**: Standardized CI build environment used across all GitHub Actions workflows.

**Release Process Role**:
- Built by `build-docker-dev.yml` workflow when dependencies change
- Provides consistent build environment across AMD64/ARM64 runners
- Used by all compilation, testing, and validation workflows
- Eliminates "works on my machine" issues in CI/CD pipeline

**Comprehensive Toolchain**:
```dockerfile
# Complete development environment including:
# - Rust toolchain with cargo
# - Java 21 JRE + Maven for SQL compiler
# - Node.js + Bun for web console builds
# - AWS CLI for deployment testing
# - Docker for integration testing
# - Testing tools: jq, curl, git, strace
```

**Workflow Integration**:
- Referenced in 8+ workflow files with specific SHA hashes
- Updated through manual process when new dependencies are added
- Ensures reproducible builds across different runner environments
- Supports both development and release build scenarios

### **Demo Environment** (`Dockerfile.demo`)

**Purpose**: Self-contained environment for running all Feldera demos and tutorials.

**Release Process Role**:
- Validates release functionality through comprehensive demo testing
- Used in demo profiles for customer onboarding and presentations
- Ensures demos work consistently across different environments
- Part of release validation process

**Capabilities**:
```dockerfile
# Includes all demo dependencies:
# - Python client libraries and demo scripts
# - RPK (Redpanda CLI) for Kafka operations
# - SnowSQL for data warehouse integration
# - Database connectors (PostgreSQL, MySQL)
# - Fraud detection ML dependencies
```

**Demo Integration**:
- Supports 5+ different demo profiles (MySQL, PostgreSQL, Snowflake, etc.)
- Used in integration testing for release validation
- Provides consistent demo environment for customer presentations

### **Kafka Connect Integration** (`Dockerfile.kafka-connect`)

**Purpose**: Specialized Kafka Connect container with Debezium CDC and Snowflake connectors.

**Release Process Role**:
- Enables CDC (Change Data Capture) integration testing
- Validates database connector functionality in release pipeline
- Supports enterprise integration scenarios

**Enterprise Connectors**:
```dockerfile
# Based on Debezium Connect 2.7.3.Final
# Includes:
# - All Debezium CDC connectors (MySQL, PostgreSQL, etc.)
# - Snowflake Kafka Connector for data warehouse integration
# - Confluent JDBC connector for database operations
```

## Docker Compose Orchestration

### **Base Production Stack** (`docker-compose.yml`)

**Purpose**: Core production deployment configuration using released container images.

**Release Process Role**:
- Primary configuration referenced in quickstart documentation
- Uses versioned container images from GitHub Container Registry
- Provides stable deployment target for production environments
- Updated during release process with new image tags

**Core Services**:
```yaml
services:
  pipeline-manager:
    image: ghcr.io/feldera/pipeline-manager:${FELDERA_VERSION:-latest}
    # Production-ready configuration with health checks
    # CORS configuration for web console access
    # Authentication support (OAuth2/OIDC)

  redpanda:
    # Kafka-compatible streaming platform
    # Used across multiple demo profiles
    # Configured for development (single-node cluster)
```

**Profile System**:
- `redpanda`: Kafka streaming infrastructure
- `prometheus`: Metrics collection and monitoring
- `grafana`: Observability dashboards
- Multiple demo profiles for different integration scenarios

### **Development Override** (`docker-compose-dev.yml`)

**Purpose**: Overrides base configuration to build from local source code.

**Development Workflow Role**:
- Used by developers for local testing with uncommitted changes
- Referenced in development sections of deployment documentation
- Enables rapid iteration without pushing to container registry
- Critical for pre-release testing and validation

**Key Override**:
```yaml
services:
  pipeline-manager:
    build:
      context: ../
      dockerfile: deploy/Dockerfile
    # Builds fresh container from current source tree
    # Maintains all other production configuration
```

### **Extended Services** (`docker-compose-extra.yml`)

**Purpose**: Additional infrastructure services for comprehensive testing and demos.

**Integration Testing Role**:
- Provides MySQL, PostgreSQL databases for connector testing
- Kafka Connect service for CDC integration validation
- Required for database integration demos and tutorials
- Used in CI integration tests

**Infrastructure Services**:
```yaml
services:
  mysql:
    # MySQL 8.0 with CDC configuration
    # Enables Debezium change capture testing

  postgres:
    # PostgreSQL with replication configuration
    # Supports CDC and direct connector testing

  kafka-connect:
    # Debezium + Snowflake connector infrastructure
    # Enables end-to-end CDC pipeline testing
```

### **Demo Orchestration** (`docker-compose-demo.yml`)

**Purpose**: Complete demo scenarios showcasing Feldera capabilities.

**Customer Onboarding Role**:
- Provides working examples of Feldera integration patterns
- Validates end-to-end functionality in release testing
- Supports customer proof-of-concept deployments
- Demonstrates CDC, streaming analytics, and data warehouse integration

**Demo Profiles**:
```yaml
profiles:
  demo-debezium-mysql:
    # Complete CDC pipeline: MySQL → Kafka → Feldera → Analytics
    # Showcases real-time change capture and processing

  demo-supply-chain-tutorial:
    # Business-focused demo with supply chain analytics
    # Demonstrates SQL streaming analytics capabilities

  demo-snowflake-sink:
    # Data warehouse integration showcase
    # End-to-end pipeline to Snowflake data warehouse
```

## CI/CD Integration Points

### **Build Pipeline Integration**

**Image Publication Workflow**:
1. `build-docker.yml` builds production images during releases
2. Images tagged with release versions and pushed to GitHub Container Registry
3. `docker-compose.yml` updated with new version tags
4. Release artifacts published for customer consumption

**Development Environment Maintenance**:
1. `build-docker-dev.yml` rebuilds CI environment when dependencies change
2. Manual process updates workflow files with new image SHAs
3. Ensures consistent build environment across all CI workflows

### **Testing Integration**

**Multi-Stage Validation**:
- `test-integration.yml` uses docker-compose configurations for integration testing
- Demo profiles validate end-to-end functionality in release pipeline
- Database connectors tested through extended service configurations
- Multi-architecture testing (AMD64/ARM64) through specialized images

**Release Validation Process**:
1. Unit tests run in standardized CI environment (`build.Dockerfile`)
2. Integration tests validate docker-compose stack functionality
3. Demo scenarios test customer-facing capabilities
4. Production images validated before release publication

### **Release Process Integration**

**Automated Release Pipeline**:
```yaml
# ci-release.yml coordinates:
1. Build production Docker images
2. Tag with release version
3. Publish to container registry
4. Update documentation with new version tags
5. Validate through demo testing
```

**Version Management**:
- Environment variable `FELDERA_VERSION` controls image versions
- Release workflows automatically update version tags
- Production deployments can pin to specific versions
- Latest tag updated for rolling releases

## Configuration and Monitoring

### **Observability Stack**

**Prometheus Integration** (`config/prometheus.yml`):
- Scrapes metrics from pipeline manager and DBSP circuits
- Configured for development and production monitoring
- Integrated with Grafana for visualization

**Grafana Configuration** (`config/grafana_data_sources.yaml`):
- Pre-configured Prometheus data sources
- Custom dashboards for Feldera-specific metrics (`grafana_dashboard.json`)
- Production-ready observability stack

### **Development vs Production Configurations**

**Development Focus**:
- Fast iteration with local source builds
- Comprehensive logging and debugging tools
- All demo and testing infrastructure available
- Development-friendly security settings

**Production Focus**:
- Versioned, tested container images
- Security-hardened configurations
- Health checks and graceful shutdown
- Production-scale resource allocations

## Deployment Scenarios

### **Customer Self-Hosted Deployments**

**Quick Start Path**:
```bash
# Uses production images with latest stable release
docker compose -f deploy/docker-compose.yml up
```

**Development/Testing Path**:
```bash
# Builds from source for customization
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-dev.yml up --build
```

### **Enterprise Integration Scenarios**

**Database CDC Integration**:
```bash
# Complete CDC pipeline with MySQL
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-extra.yml \
               --profile mysql --profile kafka-connect up
```

**Data Warehouse Integration**:
```bash
# Snowflake integration demo
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-demo.yml \
               --profile demo-snowflake-sink up
```

## Best Practices

### **Image Management**

- Production images are multi-architecture (AMD64/ARM64)
- Version pinning for reproducible deployments
- Minimal attack surface in runtime images
- Comprehensive tooling in development images

### **Configuration Layering**

- Base configuration provides stable production foundation
- Override files enable development and testing scenarios
- Profile system allows selective service activation
- Environment variables enable runtime customization

### **Release Integration**

- All changes tested through CI before release
- Demo validation ensures customer-facing functionality works
- Version tagging provides clear upgrade paths
- Documentation automatically updated with new versions

This deployment architecture provides Feldera with a robust, scalable, and maintainable foundation for both development workflows and production deployments, while seamlessly integrating with the comprehensive CI/CD pipeline.
<!-- SECTION:deploy/CLAUDE.md END -->

---

## Context: docs.feldera.com/CLAUDE.md
<!-- SECTION:docs.feldera.com/CLAUDE.md START -->
## Overview

## Key Development Commands

### Package Management

This project uses **Yarn** as the package manager.

```bash
# Install dependencies
yarn

# Start development server
yarn start

# Build static site
yarn build

# Serve built site locally
yarn serve

# Clear Docusaurus cache
yarn clear

# Type check
yarn typecheck
```

## Architecture Overview

### Technology Stack

- **Static Site Generator**: Docusaurus 3.8+ for modern documentation sites
- **Content**: MDX (Markdown + React) for interactive documentation
- **API Docs**: OpenAPI integration with docusaurus-preset-openapi
- **Analytics**: PostHog integration for user analytics
- **Styling**: Custom CSS with Docusaurus theming
- **Deployment**: Automatic deployment to docs.feldera.com

### Project Structure

#### Core Directories

- `docs/` - Main documentation content in MDX format
- `src/` - React components and custom styling
- `static/` - Static assets (images, PDFs, Python docs)
- `openapi/` - OpenAPI specification for REST API docs
- `build/` - Generated static site output

#### Content Organization

- `docs/get-started/` - Installation and quickstart guides
- `docs/sql/` - SQL reference and examples
- `docs/connectors/` - Data source and sink connectors
- `docs/tutorials/` - Step-by-step tutorials
- `docs/use_cases/` - Real-world use case examples
- `docs/interface/` - Web console and CLI documentation

## Important Implementation Details

### Documentation Features

- **Interactive Examples**: Sandbox integration for live SQL demos
- **API Documentation**: Auto-generated from OpenAPI specification
- **Python SDK Docs**: Generated Python documentation integration
- **Multi-format Content**: Support for diagrams, videos, and interactive components

### Custom Components

- **Sandbox Button**: Direct integration with Feldera sandbox for trying examples
- **Code Blocks**: Enhanced code blocks with copy functionality
- **Custom Styling**: Branded theme with Feldera colors and typography

### Build Process

The build process includes:
1. Copy OpenAPI specification from parent directory
2. Generate static site with Docusaurus
3. Include Python documentation from `/static/python/`
4. Process MDX components and custom React components

### Deployment

- **Automatic Deployment**: Configured to deploy automatically on releases
- **Domain**: Deploys to docs.feldera.com
- **CDN**: Static hosting with global CDN distribution

## Development Workflow

### For Content Changes

1. Edit MDX files in `docs/` directory
2. Start development server with `yarn start`
3. Preview changes in browser (auto-reload enabled)
4. Test build with `yarn build`
5. Commit changes for automatic deployment

### For Component Changes

1. Modify React components in `src/` directory
2. Update styling in `src/css/custom.css`
3. Test components in development mode
4. Ensure responsive design works across devices

### Testing Strategy

- **Local Development**: Live reload for content iteration
- **Build Validation**: Ensure clean builds without errors
- **Link Checking**: Docusaurus validates internal links
- **Cross-browser**: Test across different browsers and devices

### Content Management

#### Writing Guidelines
- Use MDX for interactive content combining Markdown and React
- Include code examples with syntax highlighting
- Add sandbox buttons for executable SQL examples
- Maintain consistent structure and navigation

#### Asset Management
- Store images and diagrams in appropriate `docs/` subdirectories
- Use descriptive filenames for assets
- Optimize images for web delivery
- Include alt text for accessibility

### Configuration Files

- `docusaurus.config.ts` - Main Docusaurus configuration
- `sidebars.ts` - Navigation sidebar configuration
- `package.json` - Dependencies and build scripts
- `tsconfig.json` - TypeScript configuration

### Dependencies

#### Core Dependencies
- `@docusaurus/core` - Static site generator core
- `@docusaurus/preset-classic` - Standard documentation features
- `docusaurus-preset-openapi` - API documentation integration
- `@mdx-js/react` - MDX component support

#### Analytics and Plugins
- `posthog-docusaurus` - User analytics integration
- `docusaurus-plugin-hubspot` - Marketing integration
- `react-lite-youtube-embed` - Embedded video support

### Best Practices

#### Content Creation
- **Clear Structure**: Organize content logically with proper headings
- **Interactive Examples**: Include runnable code examples where possible
- **Visual Aids**: Use diagrams and screenshots to illustrate concepts
- **Cross-references**: Link related content appropriately

#### Performance
- **Optimized Images**: Compress images and use appropriate formats
- **Lazy Loading**: Leverage Docusaurus lazy loading for better performance
- **Bundle Size**: Monitor JavaScript bundle size for fast loading

#### SEO and Accessibility
- **Meta Tags**: Proper meta descriptions and titles
- **Alt Text**: Include alt text for all images
- **Semantic HTML**: Use proper heading hierarchy
- **Mobile Friendly**: Ensure responsive design

This documentation site serves as the primary resource for Feldera users, providing comprehensive guides, tutorials, and API reference materials.
<!-- SECTION:docs.feldera.com/CLAUDE.md END -->

---

## Context: python/CLAUDE.md
<!-- SECTION:python/CLAUDE.md START -->
## Overview

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
- **OIDC Authentication**: `testutils_oidc.py` - OIDC token management and authentication utilities (see `tests/CLAUDE.md` for details)

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
- `PyJWT>=2.8.0` - JWT token handling for OIDC authentication
- `pretty-errors` - Enhanced error formatting
- `ruff>=0.6.9` - Fast Python linter and formatter

#### Development Dependencies
- `pytest>=8.3.5` - Testing framework with timeout support
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
<!-- SECTION:python/CLAUDE.md END -->

---

## Context: python/tests/CLAUDE.md
<!-- SECTION:python/tests/CLAUDE.md START -->
# Python Tests

This directory contains the comprehensive test suite for the Feldera platform.

## Authentication Setup for Platform Tests (`python/`)

This directory contains integration tests for the Feldera Python SDK and platform integration.

### OIDC Authentication (Recommended for CI)

Platform tests in the `platform/` subdirectory support OIDC authentication via environment variables. This is the primary authentication method used in CI environments.

#### Environment Variables

```bash
export OIDC_TEST_ISSUER="https://your-oidc-provider.com"
export OIDC_TEST_CLIENT_ID="your-client-id"
export OIDC_TEST_CLIENT_SECRET="your-client-secret"
export OIDC_TEST_USERNAME="testuser@example.com"
export OIDC_TEST_PASSWORD="test-password"
export OIDC_TEST_SCOPE="openid profile email"  # Optional, defaults shown
```

#### Token Caching Architecture

The OIDC authentication system uses **pytest-xdist hooks** and **environment variables** to guarantee exactly one ROPG authentication request per test session, regardless of the number of parallel workers (`pytest -n 24`).

##### Master-Worker Token Distribution

The implementation leverages pytest-xdist's master/worker communication and cross-process environment variables:

- **Master Node Only**: OIDC token fetching occurs exclusively on the pytest master node via `pytest_configure` hook
- **Environment Variable Storage**: Token data is stored in `FELDERA_PYTEST_OIDC_TOKEN` environment variable for cross-process access
- **Zero Race Conditions**: No file locking or inter-process synchronization needed - environment variables are inherited by child processes
- **Guaranteed Once-Only**: Exactly one auth request per test session, even with unlimited parallel workers

This completely eliminates auth server rate limiting issues and cross-process token sharing problems that occurred with previous approaches.

##### Authentication Flow

1. **Master Hook Execution**: `pytest_configure()` runs only on master node and fetches OIDC token once
2. **Environment Storage**: Token data is base64-encoded and stored in `FELDERA_PYTEST_OIDC_TOKEN` environment variable
3. **Cross-Process Access**: All worker processes inherit the environment variable automatically
4. **Token Retrieval**: `obtain_access_token()` reads and parses token from environment variable
5. **Fail-Fast**: Authentication failures prevent all tests from running with clear error messages

##### Implementation Details

The OIDC token caching system uses pytest-xdist hooks and environment variables for cross-process token sharing:

- **Master-Only Fetching**: `pytest_configure()` hook fetches OIDC token once on master node
- **Environment Storage**: Token cached in `FELDERA_PYTEST_OIDC_TOKEN` with base64 encoding
- **Cross-Process Access**: Worker processes inherit environment variable automatically
- **Session Validation**: `oidc_token_fixture` verifies token setup with 30-second expiration buffer
- **Header Integration**: `http_request()` merges authentication headers with custom headers

**Key Components**:
- **`pytest_configure()` (conftest.py)**: Master-only hook that fetches OIDC token once and stores in environment
- **`pytest_configure_node()` (conftest.py)**: xdist hook that passes token data via workerinput (backup mechanism)
- **`oidc_token_fixture()` (conftest.py)**: Session fixture that verifies token setup
- **`obtain_access_token()` (testutils_oidc.py)**: Returns token from environment variable or fails fast
- **`http_request()` (helper.py)**: Merges authentication headers with custom headers for ingress/egress requests
- **Reusable Token Functions (testutils_oidc.py)**:
  - `setup_token_cache()`: High-level function that sets up token caching (used by both pytest and demo runners)
  - `get_cached_token_from_env()`: Retrieves and validates cached tokens
  - `parse_cached_token()`: Parses base64-encoded token data
  - `is_token_valid()`: Checks token expiration with configurable buffer
  - `encode_token_for_env()`: Encodes tokens for environment storage

### API Key Authentication (Fallback)

If OIDC environment variables are not configured, tests fall back to API key authentication:

```bash
export FELDERA_API_KEY="your-api-key"
```

### Usage in Tests

Platform tests automatically detect and use the appropriate authentication method via `_base_headers()` helper function.

Authentication priority:
1. **OIDC** (if `OIDC_TEST_ISSUER` and related vars are set)
2. **API Key** (if `FELDERA_API_KEY` is set)
3. **No Auth** (for local testing without authentication)

### CI Configuration

In GitHub Actions workflows, OIDC authentication is configured via repository variables and secrets:

```yaml
env:
  OIDC_TEST_ISSUER: ${{ vars.OIDC_TEST_ISSUER }}
  OIDC_TEST_CLIENT_ID: ${{ vars.OIDC_TEST_CLIENT_ID }}
  OIDC_TEST_CLIENT_SECRET: ${{ secrets.OIDC_TEST_CLIENT_SECRET }}
  OIDC_TEST_USERNAME: ${{ secrets.OIDC_TEST_USERNAME }}
  OIDC_TEST_PASSWORD: ${{ secrets.OIDC_TEST_PASSWORD }}
```

This ensures consistent authentication across both "Runtime Integration Tests" and "Platform Integration Tests (OSS Docker Image)" workflows that run in parallel.

### OIDC Usage Beyond Testing

The OIDC infrastructure is designed to be reusable outside of the test suite:

#### Demo Runners and External Tools

External tools can reuse the OIDC infrastructure by calling `setup_token_cache()` followed by `_get_effective_api_key()` to get cached tokens or fallback API keys.

#### Token Caching for Multiple Processes

The token caching system is designed to work across multiple processes:

1. **First Process**: Calls `setup_token_cache()` → fetches and caches token in environment
2. **Subsequent Processes**: Call `setup_token_cache()` → reuses cached token if still valid
3. **Automatic Refresh**: Fetches new token only when cached token expires

This pattern is used by:
- **Pytest Test Runs**: Master node fetches token, workers reuse it
- **Demo Runners**: `demo/all-packaged/run.py` uses the same caching mechanism
- **CI Workflows**: Multiple demos in sequence reuse the same token
<!-- SECTION:python/tests/CLAUDE.md END -->

---

## Context: scripts/CLAUDE.md
<!-- SECTION:scripts/CLAUDE.md START -->
## Overview

The scripts directory contains scripts commonly used throughout the repository.

## `claude.js` — Merge and Split CLAUDE.md Files

This script manages all `CLAUDE.md` files in the repository by **merging** them into a single root-level `CLAUDE.md` for centralized AI context, and **splitting** them back into their original locations when needed.

### **Commands**
- **Merge**
  - Combines all scattered `CLAUDE.md` files into `<root>/CLAUDE.md`.
  - Each section is wrapped in `<!-- SECTION:<path> START -->` / `<!-- SECTION:<path> END -->` markers, preserving the file’s original relative path (e.g., `benchmark/CLAUDE.md`).
  - If a root `CLAUDE.md` exists before merging, its content is saved as a special “root section” so it can be restored later.
  - Deletes the original scattered files after merging, leaving only the merged root file.

- **Split**
  - Recreates all scattered `CLAUDE.md` files from the merged root file’s sections.
  - Restores the root `CLAUDE.md` content from its saved “root section” if present, or empties it if not.
  - Preserves directory structure when restoring files.

### **Usage**
Run from any subdirectory (e.g., `/web-console`) and optionally pass the repo root path:

```bash
bun ../scripts/claude.js merge --root ..
bun ../scripts/claude.js split --root ..
```

- If `--root` is omitted, the script assumes the repo root is the parent of `/scripts/`.
- Dot-directories like `.github/` are ignored unless explicitly whitelisted in the script.

### **Why Markers?**
The comment markers ensure that:
- Claude can see and reason about each section’s origin path in the raw file.
- The split operation can exactly reconstruct the original files and their locations.
<!-- SECTION:scripts/CLAUDE.md END -->

---

## Context: sql-to-dbsp-compiler/CLAUDE.md
<!-- SECTION:sql-to-dbsp-compiler/CLAUDE.md START -->
## Overview

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
<!-- SECTION:sql-to-dbsp-compiler/CLAUDE.md END -->

---

## Context: sql-to-dbsp-compiler/SQL-compiler/CLAUDE.md
<!-- SECTION:sql-to-dbsp-compiler/SQL-compiler/CLAUDE.md START -->
## Overview

The SQL-compiler is a sophisticated Java-based compilation system that transforms SQL queries into optimized DBSP (Database Stream Processor) circuits for incremental computation. It implements a complete compilation pipeline from SQL parsing through DBSP circuit generation to Rust code emission, enabling high-performance incremental view maintenance.

## Architecture Overview

### **Three-Phase Compilation Pipeline**

The compiler follows a structured **frontend → midend → backend** architecture:

1. **Frontend (SQL → RelNode)**: Apache Calcite-based SQL parsing, validation, and relational algebra conversion
2. **Midend (RelNode → DBSP IR)**: Transformation from relational algebra to DBSP circuit intermediate representation
3. **Backend (DBSP IR → Rust)**: Code generation producing optimized Rust implementations with multi-crate support

### **Core Design Principles**

- **Incremental Semantics**: Transforms batch SQL operations into change-propagating streaming equivalents
- **Type Safety**: Rich type system with null safety guarantees and SQL-to-Rust type mapping
- **Visitor-Based Architecture**: Extensible transformation framework using nested visitor patterns
- **Apache Calcite Integration**: Leverages industrial-strength SQL parsing and optimization infrastructure

## Key Components and Patterns

### **Main Entry Points**

#### `CompilerMain.java`
Primary command-line interface orchestrating the compilation pipeline:
```java
// Main compilation flow
CompilerMain compiler = new CompilerMain();
compiler.compileFromFile(sqlFile, outputFile, options);
```

**Key Responsibilities**:
- Command-line argument parsing with comprehensive option support
- Input/output stream management (stdin, files, API)
- Compilation orchestration and error reporting
- Integration with development tools and IDE support

#### `DBSPCompiler.java`
Central compiler orchestrator managing compilation state and metadata:
```java
DBSPCompiler compiler = new DBSPCompiler();
Circuit circuit = compiler.compile(sqlStatements);
```

**Key Features**:
- Multi-statement SQL compilation with dependency resolution
- Metadata management for tables, views, functions, and types
- Error collection and reporting with source position tracking
- Integration points for runtime deployment and testing

### **Frontend: SQL Processing**

#### `SqlToRelCompiler.java`
Apache Calcite-based SQL frontend handling parsing and relational conversion:

**SQL Parsing Pipeline**:
1. **SqlParser** → Parse SQL text to `SqlNode` AST
2. **SqlValidator** → Type checking and semantic validation
3. **SqlToRelConverter** → Transform to `RelNode` relational algebra
4. **RelOptPlanner** → Apply Calcite-based optimization rules

**Custom SQL Extensions**:
- `SqlCreateTable` with connector properties for external data sources
- `SqlLateness` annotations for watermark specifications in streaming contexts
- `SqlCreateView` with materialization and persistence options
- Extended function support including user-defined aggregates and table functions

#### Key SQL Features Supported:
- **Complex Joins**: INNER, LEFT, RIGHT, FULL OUTER, ANTI, SEMI, AS-OF joins with optimized execution plans
- **Window Functions**: ROW_NUMBER, RANK, SUM/COUNT OVER with flexible frame specifications
- **Recursive Queries**: WITH RECURSIVE supporting fixed-point computation with convergence detection
- **Aggregations**: Standard SQL aggregates plus custom streaming-optimized variants
- **Subqueries**: Correlated and uncorrelated subqueries with efficient decorrelation

### **Midend: DBSP Circuit Generation**

#### `CalciteToDBSPCompiler.java`
Core transformation engine converting relational algebra to DBSP circuits:

**Operator Mapping Strategy**:
```java
// Relational to DBSP operator transformation
LogicalTableScan → DBSPSourceTableOperator  // Data ingestion
LogicalProject   → DBSPMapOperator          // Row transformations
LogicalFilter    → DBSPFilterOperator       // Predicate filtering
LogicalJoin      → DBSPJoinOperator         // Incremental joins
LogicalAggregate → DBSPAggregateOperator    // Streaming aggregation
LogicalWindow    → DBSPWindowOperator       // Windowed computation
```

**Incremental Computation Transformation**:
- **Change Propagation**: Converts batch semantics to delta-oriented processing
- **State Management**: Automatically introduces materialization points for intermediate results
- **Z-Set Mathematics**: Uses mathematical foundations ensuring correctness of incremental updates
- **Memory Optimization**: Minimizes operator state through dead code elimination and fusion

#### `DBSPOperator` Hierarchy
Rich operator type system supporting incremental computation:

**Core Operators**:
- `DBSPSourceTableOperator`: External data ingestion with change stream support
- `DBSPMapOperator`: Stateless row-by-row transformations with expression evaluation
- `DBSPFilterOperator`: Predicate-based filtering with incremental maintenance
- `DBSPJoinOperator`: Multi-way joins with efficient incremental update propagation
- `DBSPAggregateOperator`: Group-by aggregation with incremental aggregate maintenance
- `DBSPDistinctOperator`: Duplicate elimination with reference counting
- `DBSPWindowOperator`: Time-based and row-based windowing with expiration handling

### **Backend: Code Generation**

#### `ToRustVisitor.java`
Primary code generation engine producing optimized Rust implementations:

**Generated Code Architecture**:
```rust
// Generated circuit structure
pub fn circuit(workers: usize) -> DBSPHandle {
    let circuit = Runtime::init_circuit(workers, |circuit| {
        // Input streams with typed interfaces
        let orders = circuit.add_input_zset::<Order, isize>();

        // Operator chain with incremental semantics
        let filtered = circuit.add_unary_operator(
            Filter::new(|order: &Order| order.amount > 1000)
        );

        // Output with downstream integration
        circuit.add_output(filtered, output_sink);
    })
}
```

**Code Generation Features**:
- **Type-Safe Generation**: SQL types mapped to Rust types with null safety (`Option<T>`)
- **Multi-Crate Support**: Generates modular Rust projects with dependency management
- **Optimization Integration**: Applies circuit-level optimizations during code generation
- **Runtime Integration**: Generated code seamlessly integrates with DBSP runtime infrastructure

#### `RustFileWriter.java` & `MultiCratesWriter.java`
Flexible output generation supporting single-file and multi-crate Rust projects:

**Output Modes**:
- **Single File**: Complete circuit in one Rust file for simple deployments
- **Multi-Crate**: Structured Cargo workspace with separate crates for different concerns
- **Library Generation**: Reusable circuit components with clean API boundaries

### **Type System Implementation**

#### `DBSPType` Hierarchy
Comprehensive type system bridging SQL and Rust type systems:

**Type Categories**:
```java
// Primitive types with null safety
DBSPTypeBool, DBSPTypeInteger, DBSPTypeString, DBSPTypeDate, DBSPTypeTimestamp

// Collection types for streaming computation
DBSPTypeZSet<T>      // Change sets with multiplicities
DBSPTypeIndexedZSet  // Indexed collections for efficient joins
DBSPTypeStream<T>    // Temporal streams with timestamps

// Composite types supporting nested data
DBSPTypeTuple        // Product types for multi-column data
DBSPTypeStruct       // Named field structures
DBSPTypeMap<K,V>     // Associative collections
```

**Type Inference and Resolution**:
- **Calcite Integration**: Leverages `RelDataType` system for SQL type checking
- **Generic Support**: Template-based type generation for polymorphic operators
- **Null Propagation**: Automatic null handling with Three-Valued Logic (3VL) support
- **Type Coercion**: Automatic type conversions following SQL standard rules

## Advanced Features and Patterns

### **Optimization Framework**

#### Multi-Pass Visitor Architecture
The compiler employs a sophisticated two-level visitor pattern:

**Circuit-Level Visitors** (`CircuitVisitor`):
- `CircuitOptimizer`: Orchestrates multiple optimization passes
- `DeadCodeElimination`: Removes unreachable operators and unused computations
- `CommonSubexpressionElimination`: Shares identical subcomputations across the circuit
- `OperatorFusion`: Combines adjacent operators for reduced overhead

**Expression-Level Visitors** (`InnerVisitor`):
- `Simplify`: Algebraic simplification and constant folding
- `BetaReduction`: Lambda calculus reductions for higher-order functions
- `ConstantPropagation`: Compile-time evaluation of constant expressions

#### Key Optimization Patterns:
```java
// Operator fusion example
MapOperator(FilterOperator(source)) → FilterMapOperator(source)

// Join optimization with index selection
Join(A, B) → IndexedJoin(A, createIndex(B, joinKey))

// Aggregation optimization
GroupBy(Map(source)) → OptimizedAggregate(source)
```

### **Error Handling Architecture**

#### Structured Exception Hierarchy
```java
BaseCompilerException
├── CompilationError          // General compilation failures
├── InternalCompilerError     // Unexpected compiler states
├── UnsupportedException      // Unimplemented SQL features
└── UnimplementedException    // Missing functionality
```

**Error Context Tracking**:
- **SourcePositionRange**: Precise error location mapping to original SQL
- **CalciteObject**: Links IR nodes back to source SQL constructs
- **CompilerMessages**: Centralized error collection with severity levels

### **Integration Patterns**

#### Runtime Integration
- **Handle-Based Management**: Generated circuits managed through `DBSPHandle` interface
- **Streaming Coordination**: Automatic coordination with DBSP runtime scheduler
- **Memory Management**: Efficient data structure selection based on access patterns

#### Pipeline Manager Integration
- **REST API**: Compilation services exposed through HTTP endpoints
- **JSON Metadata**: Schema generation for runtime circuit introspection
- **Serialization**: Circuit persistence for deployment and recovery

## Development Workflow

### **Key Build Commands**

```bash
# Build the compiler
mvn clean compile

# Run tests with coverage
mvn test -Pcoverage

# Generate code from SQL file
java -jar SQL-compiler.jar --input queries.sql --output circuit.rs

# Multi-crate generation
java -jar SQL-compiler.jar --input queries.sql --outputDir ./output --multiCrate
```

### **Testing Strategy**

#### Test Categories:
- **Unit Tests**: Individual component testing with mocked dependencies
- **Integration Tests**: End-to-end SQL compilation with runtime validation
- **Regression Tests**: Comprehensive test suite preventing compilation regressions
- **Performance Tests**: Compilation time and generated code efficiency validation

#### Key Test Patterns:
```java
@Test
public void testComplexJoin() {
    String sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id";
    Circuit circuit = compiler.compile(sql);

    // Validate circuit structure
    assertThat(circuit.getOperators()).hasSize(3);
    assertThat(circuit.hasOperator(DBSPJoinOperator.class)).isTrue();
}
```

### **Debugging and Diagnostics**

#### Compiler Introspection:
- **Circuit Visualization**: Generate GraphViz diagrams of operator graphs
- **Type Debugging**: Detailed type inference traces with constraint solving
- **Optimization Traces**: Step-by-step optimization pass logging
- **Generated Code Inspection**: Formatted Rust output with debug annotations

## Best Practices

### **Adding New SQL Features**

1. **Extend Calcite Parser**: Add new SQL syntax to parser grammar
2. **Implement SqlNode**: Create AST node for new SQL construct
3. **Add Validation**: Implement semantic validation rules
4. **Create DBSP Operator**: Design incremental operator semantics
5. **Implement Transformation**: Add RelNode to DBSP conversion
6. **Generate Code**: Implement Rust code generation
7. **Add Tests**: Comprehensive test coverage including edge cases

### **Performance Optimization**

- **Profile Generated Code**: Use `cargo bench` to measure generated circuit performance
- **Optimize Hot Paths**: Focus optimization on frequently executed operators
- **Memory Profiling**: Monitor memory usage in long-running incremental computations
- **Parallel Execution**: Leverage DBSP's multi-threading capabilities

### **Error Handling**

- **Precise Error Messages**: Include SQL source positions and suggested fixes
- **Graceful Degradation**: Continue compilation where possible, collecting multiple errors
- **Context Preservation**: Maintain connection between IR nodes and original SQL
- **User-Friendly Output**: Format error messages for developer consumption

This SQL-to-DBSP compiler represents a sophisticated piece of compiler engineering that successfully bridges the gap between declarative SQL and high-performance incremental computation. Its layered architecture, extensive use of design patterns, and integration with both Apache Calcite and the DBSP runtime create a powerful platform for incremental view maintenance at scale.
<!-- SECTION:sql-to-dbsp-compiler/SQL-compiler/CLAUDE.md END -->

---

## Context: web-console/CLAUDE.md
<!-- SECTION:web-console/CLAUDE.md START -->
## Key Development Commands

### Package Manager

This project uses **Bun** as the package manager, not npm.

```bash
# Install dependencies
bun install

# Development server
bun run dev

# Build the application
bun run build

# Code formatting and linting
bun run format     # Format code with Prettier
bun run lint       # Lint with ESLint
bun run check      # Type check with svelte-check

# Testing
bun run test-e2e      # End-to-end tests with Playwright
bun run test-e2e-ui   # E2E tests with UI
bun run test-ct       # Component tests
bun run test-ct-ui    # Component tests with UI

# OpenAPI generation (important for API updates)
bun run build-openapi     # Generate new openapi.json from Rust backend
bun run generate-openapi  # Generate TypeScript client from openapi.json

# Icon packs webfont generation
bun run build-icons
```

## Architecture Overview

Web Console is a dasboard UI app deployed as a static website that is served by the pipeline-manager service in a Feldera deployment. The app is self-contained and does not need an internet connection to function properly.

### Technology Stack

- **Frontend**: SvelteKit 2.x with Svelte 5 (using runes)
- **Language**: TypeScript with strict configuration
- **Styling**: TailwindCSS + Skeleton UI components
- **Build Tool**: Vite with ESBuild minification
- **Authentication**: OIDC/OAuth2 via @axa-fr/oidc-client
- **HTTP Client**: @hey-api/client-fetch with auto-generated TypeScript bindings
- **Testing**: Playwright for E2E and component testing
- **Package Manager**: Bun (not npm)

### Project Structure

#### Core Directories

- `src/lib/components/` - Reusable Svelte components organized by feature
- `src/lib/compositions/` - Stateful functions for app state management (Svelte runes-based), organized by feature family where applicable
- `src/lib/functions/` - Pure functions and utilities, organized by application
- `src/lib/services/` - API services and side effects (networking, storage)
- `src/lib/types/` - TypeScript type definitions
- `src/routes/` - File-based routing (SvelteKit pages)

#### Key Service Layer

- **Pipeline Manager API**: Main backend communication via `src/lib/services/pipelineManager.ts` which wraps the auto-generated API client
- **Auto-generated API client**: Located in `src/lib/services/manager/` (generated from OpenAPI spec)
- **HTTP Client Setup**: Global client configuration in `src/lib/compositions/setupHttpClient.ts`

#### Architecture Patterns

- **State Management**: Uses Svelte 5 runes (`$state`, `$derived`) via composition functions
- **Error Handling**: Centralized request error and occasional client error reporting through toast notifications
- **Authentication**: OIDC integration with token-based API authentication
- **Real-time Features**: WebSocket/streaming connections for pipeline logs and data egress

### Important Implementation Details

#### Authentication Flow

- Uses OIDC/OAuth2 with @axa-fr/oidc-client
- Handles login redirects and session management
- Injects auth tokens into HTTP requests via interceptors
- Network health tracking for connection status

#### Pipeline Management

- Core feature: managing data processing pipelines
- Real-time status tracking and updates
- Streaming data interfaces for logs and output
- Complex state consolidation (program status vs deployment status)

#### Code Generation

- OpenAPI TypeScript client is auto-generated - do not manually edit files in `src/lib/services/manager/`
- Icon fonts are generated from SVG files - use `bun run build-icons` after adding new icons
- API schema updates require running `bun run build-openapi && bun run generate-openapi`

### Development Workflow

#### For API Changes

1. Update Rust backend API
2. Run `bun run build-openapi` to generate new OpenAPI spec
3. Run `bun run generate-openapi` to update TypeScript client
4. Update frontend code to use new API

#### For UI Components

- Use Skeleton UI components as base layer
- Follow existing component patterns in `src/lib/components/`
- Use Svelte 5 runes syntax (`$state`, `$derived`, `$effect`)
- Implement proper TypeScript typing

#### Testing Strategy

- E2E tests for critical user workflows
- Component tests for isolated UI testing
- Use Playwright for both E2E and component testing
- Tests expect pipeline manager backend to be running on localhost:8080

### Configuration Files

- `svelte.config.js` - SvelteKit configuration with static adapter
- `vite.config.ts` - Build configuration with plugins for SVG and virtual modules
- `tailwind.config.ts` - Custom theme configuration with Skeleton integration
- `eslint.config.js` - Flat config with TypeScript and Svelte support
- `playwright.config.ts` - Test configuration
<!-- SECTION:web-console/CLAUDE.md END -->

---
