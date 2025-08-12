# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the benchmark directory in this repository.

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