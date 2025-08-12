# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Feldera repository as a whole.

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