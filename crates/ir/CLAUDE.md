# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Intermediate Representation (IR) crate.

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