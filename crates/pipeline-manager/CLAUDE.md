# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Pipeline Manager crate.

## Key Development Commands

### Building and Testing

```bash
# Build the pipeline manager
cargo build -p pipeline-manager

# Run tests
cargo test -p pipeline-manager

# Run integration tests
cargo test -p pipeline-manager --test integration_test

# Build with all features
cargo build -p pipeline-manager --all-features
```

### Running the Service

```bash
# Run the pipeline manager server
cargo run -p pipeline-manager

# Run with specific configuration
RUST_LOG=debug cargo run -p pipeline-manager -- --config config.toml

# Run database migrations
cargo run -p pipeline-manager -- --migrate-database

# Dump OpenAPI specification
cargo run -p pipeline-manager -- --dump-openapi
```

### Development Tools

```bash
# Run with hot reload using cargo-watch
cargo watch -x "run -p pipeline-manager"

# Check database connectivity
cargo run -p pipeline-manager -- --probe-db

# Generate banner
cargo run -p pipeline-manager -- --print-banner
```

## Architecture Overview

### Technology Stack

- **Web Framework**: Actix-web for HTTP server
- **Database**: PostgreSQL with SQLx for async database operations
- **Authentication**: JWT-based authentication with configurable providers
- **API Documentation**: OpenAPI/Swagger specification generation
- **Build Tools**: Custom build scripts for banner generation

### Service Components

- **API Server**: RESTful HTTP API for pipeline management
- **Database Layer**: PostgreSQL storage for pipelines, programs, and metadata
- **Compiler Integration**: SQL-to-DBSP and Rust compilation orchestration
- **Runner Service**: Pipeline execution and lifecycle management
- **Authentication**: Multi-provider authentication system

### Project Structure

#### Core Directories

- `src/api/` - HTTP API endpoints and request handling
- `src/db/` - Database operations and schema management
- `src/compiler/` - SQL and Rust compilation integration
- `src/runner/` - Pipeline execution and management
- `src/auth.rs` - Authentication and authorization
- `migrations/` - Database migration scripts

#### Key Components

- **API Endpoints**: CRUD operations for pipelines and programs
- **Database Abstraction**: Type-safe database operations
- **Compiler Services**: Integration with SQL-to-DBSP compiler
- **Pipeline Execution**: Runtime management and monitoring

## Important Implementation Details

### Database Schema

The service uses PostgreSQL with versioned migrations:

- **Programs**: SQL program definitions and compilation status
- **Pipelines**: Pipeline configurations and runtime state
- **API Keys**: Authentication credentials and permissions
- **Tenants**: Multi-tenancy support

### API Structure

#### Core Endpoints

```
GET    /api/programs           - List programs
POST   /api/programs           - Create program
GET    /api/programs/{id}      - Get program details
PATCH  /api/programs/{id}      - Update program
DELETE /api/programs/{id}      - Delete program

GET    /api/pipelines          - List pipelines
POST   /api/pipelines          - Create pipeline
GET    /api/pipelines/{id}     - Get pipeline details
PATCH  /api/pipelines/{id}     - Update pipeline
DELETE /api/pipelines/{id}     - Delete pipeline
```

#### Pipeline Lifecycle

```
POST /api/pipelines/{id}/start    - Start pipeline
POST /api/pipelines/{id}/pause    - Pause pipeline
POST /api/pipelines/{id}/shutdown - Stop pipeline
GET  /api/pipelines/{id}/stats    - Get runtime statistics
```

### Compilation Pipeline

1. **SQL Parsing**: Validate SQL program syntax
2. **DBSP Generation**: Convert SQL to DBSP circuit
3. **Rust Compilation**: Compile generated Rust code
4. **Binary Packaging**: Create executable pipeline binary
5. **Deployment**: Deploy and manage pipeline execution

### Authentication System

- **JWT Tokens**: Stateless authentication
- **API Keys**: Service-to-service authentication
- **Multi-tenant**: Tenant isolation and resource management
- **RBAC**: Role-based access control

## Development Workflow

### For API Changes

1. Modify endpoint handlers in `src/api/endpoints/`
2. Update database operations in `src/db/operations/`
3. Add/update database migrations in `migrations/`
4. Update OpenAPI specification
5. Add integration tests

### For Database Changes

1. Create migration in `migrations/V{n}__{description}.sql`
2. Update corresponding types in `src/db/types/`
3. Modify database operations in `src/db/operations/`
4. Test migration rollback scenarios
5. Update integration tests

### Testing Strategy

#### Unit Tests
- Database operation testing with test fixtures
- API endpoint testing with mock dependencies
- Authentication and authorization testing

#### Integration Tests
- End-to-end API testing with real database
- Pipeline lifecycle testing
- Multi-tenant isolation verification

### Configuration Management

The service supports multiple configuration sources:

- **Environment Variables**: Runtime configuration
- **Configuration Files**: TOML-based configuration
- **Command Line Arguments**: Override configuration
- **Database Configuration**: Dynamic configuration storage

### Error Handling

- **Structured Errors**: Type-safe error propagation
- **HTTP Error Mapping**: Appropriate HTTP status codes
- **Database Error Handling**: Transaction rollback and recovery
- **Compilation Error Reporting**: Detailed error messages

### Configuration Files

- `Cargo.toml` - Package configuration with database features
- `build.rs` - Build-time banner and asset generation
- `migrations/` - Database schema evolution
- `openapi.json` - Generated API specification

### Key Features

- **Multi-tenancy**: Isolated environments per tenant
- **High Availability**: Database connection pooling and retry logic
- **Monitoring**: Structured logging and metrics
- **Security**: Input validation and SQL injection prevention

### Dependencies

#### Core Dependencies
- `actix-web` - HTTP server framework
- `sqlx` - Async PostgreSQL client
- `serde` - Serialization/deserialization
- `tokio` - Async runtime

#### Database Dependencies
- `sqlx-postgres` - PostgreSQL driver
- `uuid` - UUID generation
- `chrono` - Date/time handling

#### Authentication Dependencies
- `jsonwebtoken` - JWT token handling
- `argon2` - Password hashing
- `oauth2` - OAuth2 client support

### Performance Considerations

- **Connection Pooling**: Database connection management
- **Async Processing**: Non-blocking I/O operations
- **Caching**: In-memory caching of frequently accessed data
- **Batch Operations**: Efficient bulk database operations

### Security Best Practices

- **Input Validation**: Comprehensive request validation
- **SQL Injection Prevention**: Parameterized queries
- **Authentication**: Secure token management
- **Authorization**: Fine-grained access control
- **Audit Logging**: Security event tracking

### Development Tools Integration

- **Hot Reload**: Development server with automatic restart
- **Database Migrations**: Version-controlled schema changes
- **OpenAPI Generation**: Automatic API documentation
- **Banner Generation**: Build-time asset creation

### Monitoring and Observability

- **Structured Logging**: JSON-formatted log output
- **Metrics Collection**: Performance and usage metrics
- **Health Checks**: Service health monitoring endpoints
- **Distributed Tracing**: Request tracing across services