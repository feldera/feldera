# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Data Generation crate.

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