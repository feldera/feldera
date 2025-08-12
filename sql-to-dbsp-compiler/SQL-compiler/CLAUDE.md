# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the SQL-compiler directory in the sql-to-dbsp-compiler project.

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