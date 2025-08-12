# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the REST API crate.

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