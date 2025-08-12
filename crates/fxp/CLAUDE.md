# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Fixed Point (FXP) crate.

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