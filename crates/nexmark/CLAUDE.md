# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the NEXMark crate.

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