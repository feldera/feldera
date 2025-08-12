# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the FDA (Feldera Development Assistant) crate.

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