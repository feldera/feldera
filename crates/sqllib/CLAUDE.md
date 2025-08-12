# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the SQL Library crate.

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