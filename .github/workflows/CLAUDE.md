# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with GitHub Actions workflows in this repository.

## Overview

The `.github/workflows/` directory contains GitHub Actions workflows that form a comprehensive CI/CD ecosystem for the Feldera platform. These workflows orchestrate a sophisticated development pipeline that spans multiple languages, platforms, and deployment targets.

### Workflow Ecosystem Architecture

The Feldera CI/CD pipeline is designed around a **hierarchical workflow architecture** with clear separation of concerns:

#### **Primary Orchestration Layer**
- **`ci.yml`** serves as the main orchestrator for pull requests and main branch changes
- **`ci-pre-mergequeue.yml`** provides fast feedback for pull request validation
- **`ci-release.yml`** coordinates the complete release process
- **`ci-post-release.yml`** handles post-release tasks like package publishing

#### **Build Foundation Layer**
- **`build-rust.yml`** - Multi-platform Rust compilation (AMD64/ARM64)
- **`build-java.yml`** - Java-based SQL compiler and Calcite integration
- **`build-docs.yml`** - Documentation site generation with Docusaurus
- **`build-docker.yml`** - Production Docker images for deployment
- **`build-docker-dev.yml`** - Development environment images for CI

#### **Testing Validation Layer**
- **`test-unit.yml`** - Comprehensive unit testing across all components
- **`test-integration.yml`** - Docker functionality and network isolation testing
- **`test-adapters.yml`** - Multi-architecture I/O adapter validation
- **`test-java.yml`** / **`test-java-nightly.yml`** - SQL compiler testing with extended nightly runs

#### **Quality Assurance Layer**
- **`docs-linkcheck.yml`** - Daily documentation link validation
- **`check-failures.yml`** - Slack notifications for critical workflow failures

#### **Publication Layer**
- **`publish-crates.yml`** - Rust crate publishing to crates.io
- **`publish-python.yml`** - Python SDK publishing to PyPI

### Key Architectural Principles

#### **Multi-Platform First**
- Native AMD64 and ARM64 support across the entire pipeline
- Uses Kubernetes-based runners (`k8s-runners-amd64`, `k8s-runners-arm64`) for scalable execution
- Ensures Feldera runs consistently across different hardware architectures

#### **Containerized Execution**
- Most workflows run in the standardized `feldera-dev` container
- Provides consistent build environments across different runner types
- Eliminates "works on my machine" issues through environment standardization

#### **Performance Optimization**
- **Caching Strategy**: Extensive use of sccache with S3-compatible storage for Rust builds
- **Parallel Execution**: Independent jobs run simultaneously to minimize total pipeline time
- **Artifact Sharing**: Build artifacts are shared between workflows to avoid redundant compilation
- **Documentation-Only Optimization**: Workflows skip expensive CI jobs when only documentation files are changed (see [Documentation-Only Change Optimization](#documentation-only-change-optimization))
- **Resource Selection**: Use appropriate runner types for different workloads

#### **Comprehensive Language Support**
- **Rust**: Core DBSP engine, adapters, and runtime components
- **Java**: SQL-to-DBSP compiler with Apache Calcite integration
- **Python**: SDK and client libraries with comprehensive testing
- **TypeScript/JavaScript**: Documentation site and web console components

#### **Release Automation**
- **Semantic Versioning**: Automated version management across all components
- **Multi-Registry Publishing**: Simultaneous publishing to crates.io, PyPI, and container registries
- **Documentation Deployment**: Automatic documentation updates for each release

#### **Quality Gates**
- **Multi-Stage Validation**: Code must pass unit tests, integration tests, and adapter tests
- **Cross-Platform Testing**: All components validated on both AMD64 and ARM64
- **Documentation Validation**: Links and content verified before release
- **External Service Testing**: Real integration testing with Kafka, PostgreSQL, Redis, and cloud services

### Workflow Interdependencies

The workflows form a **directed acyclic graph (DAG)** of dependencies:

```
ci.yml (orchestrator)
├── build-rust.yml ──────┬──> test-unit.yml
├── build-java.yml ──────┤   test-adapters.yml
├── build-docs.yml       │   test-integration.yml
├── ...
└── Dependencies ────────┴──> build-docker.yml
                              └──> publish-* workflows
```

### Development Lifecycle Integration

#### **Pull Request Flow**
1. **`ci-pre-mergequeue.yml`** provides immediate feedback
2. **`ci.yml`** runs comprehensive validation
3. Quality gates ensure code meets standards before merge

#### **Release Flow**
1. **`ci-release.yml`** creates release from specified commit
2. **`ci-post-release.yml`** publishes packages to registries
3. **`build-docker.yml`** creates versioned container images
4. **`build-docs.yml`** updates documentation site

#### **Maintenance Flow**
1. **`docs-linkcheck.yml`** runs daily to catch documentation issues
2. **`test-java-nightly.yml`** provides extended testing coverage
3. **`check-failures.yml`** monitors critical workflows and alerts team

This ecosystem provides **fast feedback loops** for developers while ensuring **comprehensive validation** and **reliable automation** for the entire Feldera platform development lifecycle.

## Workflows

### `build-docker.yml` - Docker Image Build and Push

**Purpose**: Builds and publishes Docker images for Feldera components to container registries.

**Triggers**:
- Manual workflow dispatch
- Push to main branch (for latest tags)
- Release events (for version tags)

**Key Features**:
- Multi-architecture builds (AMD64, ARM64)
- Registry support for Docker Hub, GitHub Container Registry, and AWS ECR
- Builds multiple Docker images:
  - `pipeline-manager` - Main API service
  - `sql-to-dbsp-compiler` - SQL compilation service
  - Additional utility images
- Uses BuildKit for advanced build features
- Implements layer caching for faster builds
- Tags images with version numbers and latest tags
- Supports both development and release builds

**Runners**: Uses `k8s-runners-amd64` for containerized builds with Docker support.

**Security**:
- Registry authentication via GitHub secrets
- SBOM (Software Bill of Materials) generation
- Image scanning integration

**Outputs**: Published Docker images ready for deployment in various environments.

---

### `ci.yml` - Main Continuous Integration Pipeline

**Purpose**: Orchestrates the complete CI pipeline for pull requests and main branch changes.

**Triggers**:
- Pull request events (opened, synchronized, reopened)
- Push to main branch
- Manual workflow dispatch
- Skipped if only code documentation was changed

**Key Features**:
- **Dependency Matrix**: Builds dependency graph of all CI jobs
- **Parallel Execution**: Runs multiple test suites simultaneously
- **Multi-platform Testing**: Tests across different architectures (AMD64, ARM64)
- **Comprehensive Testing**: Includes unit tests, integration tests, Java tests, and adapter tests
- **Build Validation**: Validates Rust, Java, and documentation builds
- **Artifact Management**: Collects and stores build artifacts for downstream jobs
- **Status Reporting**: Provides consolidated CI status for pull requests

**Workflow Structure**:
- **Build Phase**: Compiles Rust binaries, Java components, and documentation
- **Test Phase**: Executes comprehensive test suite in parallel
- **Integration Phase**: Runs integration tests with real services
- **Reporting Phase**: Aggregates results and reports status

**Runners**: Uses mix of `k8s-runners-amd64` and `k8s-runners-arm64` for comprehensive platform coverage.

**Dependencies**: Coordinates execution of multiple reusable workflows including build-rust, test-unit, test-integration, and others.

---

### `ci-pre-mergequeue.yml` - Pre-Merge Queue Validation

**Purpose**: Provides fast feedback for pull requests with essential validation before merge queue entry.

**Triggers**:
- Pull request events (opened, synchronized)
- Skipped if only code documentation was changed

**Key Features**:
- **Fast Feedback**: Lightweight validation for quick pull request feedback
- **Caching Optimization**: Uses sccache with S3-compatible storage for build caching
- **GitHub App Integration**: Generates tokens for secure repository access
- **Single Job Execution**: Streamlined validation in a single containerized job

**Performance Optimizations**:
- **Build Caching**: RUSTC_WRAPPER with sccache for faster Rust compilation
- **S3 Cache Backend**: Distributed caching across CI runners
- **Container Environment**: Runs in feldera-dev container with pre-installed dependencies

**Security**:
- GitHub App token generation for secure API access
- S3 credentials for cache access via GitHub secrets
- Containerized execution for isolation

**Runners**: Uses `k8s-runners-amd64` with containerized execution environment.

---

### `ci-release.yml` - Release Creation

**Purpose**: Creates new releases by dispatching the release process for specified commits.

**Triggers**:
- Repository dispatch events (trigger-oss-release)
- Manual workflow dispatch with SHA and version inputs

**Key Features**:
- **Release Coordination**: Initiates the complete release process
- **Version Management**: Handles version specification and validation
- **GitHub App Integration**: Uses app tokens for secure repository operations
- **Flexible Triggering**: Supports both automated and manual release triggers

**Input Parameters**:
- `sha_to_release`: Specific commit SHA to create release from
- `version`: Version string for the release

**Security**: Uses GitHub App tokens for repository write permissions and secure release creation.

---

### `build-rust.yml` - Rust Build Workflow

**Purpose**: Compiles Rust components across multiple platforms and architectures.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **Multi-platform Compilation**: Builds for AMD64 and ARM64 architectures
- **Optimized Builds**: Uses release mode with optimizations
- **Artifact Management**: Stores compiled binaries for downstream workflows
- **Caching**: Leverages Rust build caching for faster compilation
- **Cross-compilation**: Handles cross-platform build requirements

**Build Targets**:
- `x86_64-unknown-linux-gnu` (AMD64)
- `aarch64-unknown-linux-gnu` (ARM64)

**Outputs**: Compiled Rust binaries uploaded as GitHub Actions artifacts for use in Docker builds and testing.

---

### `test-integration.yml` - Integration Testing

**Purpose**: Runs comprehensive integration tests including network isolation, Docker functionality, and end-to-end scenarios.

**Triggers**:
- Called by other workflows (workflow_call)
- Manual workflow dispatch with run_id input

**Key Features**:
- **Network Isolation Testing**: Validates pipeline-manager works without network access
- **Docker Integration**: Tests Docker container functionality and health checks
- **Service Validation**: Ensures services start correctly and respond to health checks
- **Multi-Job Testing**: Runs various integration scenarios in parallel

**Test Scenarios**:
- `manager-no-network`: Tests pipeline-manager in isolated network environment
- Container health check validation
- Service startup and readiness verification
- Docker image functionality testing

**Infrastructure**: Uses Docker containers with custom networks for isolation testing.

---

### `build-docs.yml` - Documentation Build

**Purpose**: Builds and validates the Feldera documentation website.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **Docusaurus Build**: Compiles the static documentation site
- **Link Validation**: Ensures all internal links are valid
- **OpenAPI Integration**: Includes API documentation generation
- **Multi-format Support**: Handles MDX, images, and interactive content

**Outputs**: Static documentation site ready for deployment to docs.feldera.com.

---

### `test-unit.yml` - Unit Testing

**Purpose**: Executes comprehensive unit test suites for all Feldera components.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **Comprehensive Coverage**: Tests Rust, Python, and Java components
- **Parallel Execution**: Runs test suites in parallel for faster feedback
- **Test Reporting**: Aggregates and reports test results
- **Failure Analysis**: Provides detailed failure information

**Test Suites**:
- Rust unit tests for core DBSP functionality
- Python SDK tests
- SQL compiler tests
- Integration library tests

---

### `publish-crates.yml` & `publish-python.yml` - Package Publishing

**Purpose**: Publishes Rust crates to crates.io and Python packages to PyPI during releases.

**Triggers**:
- Called by release workflows

**Key Features**:
- **Automated Publishing**: Publishes packages with proper versioning
- **Dependency Management**: Handles cross-package dependencies correctly
- **Registry Authentication**: Securely authenticates with package registries
- **Publication Validation**: Verifies successful package publication

---

### `test-adapters.yml` - Adapter Testing

**Purpose**: Tests I/O adapters across multiple architectures with real external services.

**Triggers**:
- Called by other workflows (workflow_call)
- Manual workflow dispatch with run_id input

**Key Features**:
- **Multi-architecture Testing**: Runs on both AMD64 and ARM64 platforms
- **Service Integration**: Tests with PostgreSQL and other external services
- **Container Environment**: Runs in feldera-dev container with full service stack
- **Matrix Strategy**: Tests adapter compatibility across different architectures

**Test Infrastructure**:
- PostgreSQL 15 service for database testing
- Containerized test execution environment
- Multi-platform validation (x86_64 and aarch64)
- Service health verification and connectivity testing

**Architecture Coverage**: Ensures adapter functionality works consistently across AMD64 and ARM64 platforms.

---

### `docs-linkcheck.yml` - Documentation Link Validation

**Purpose**: Validates all links in the documentation to ensure they remain accessible and correct.

**Triggers**:
- Scheduled runs (daily at 16:00 UTC)
- Manual workflow dispatch

**Key Features**:
- **External Link Checking**: Uses linkchecker to validate docs.feldera.com
- **Selective Validation**: Ignores problematic domains (localhost, crates.io, LinkedIn, etc.)
- **Python Environment**: Uses uv for dependency management
- **No Robot Restrictions**: Bypasses robots.txt restrictions for thorough checking

**Ignored URLs**: Excludes localhost, crates.io, social media sites, and IEEE domains that commonly have access issues.

---

### `test-java.yml` & `test-java-nightly.yml` - Java Testing

**Purpose**: Executes Java-based SQL compiler tests with comprehensive validation.

**Triggers**:
- `test-java.yml`: Called by other workflows (workflow_call)
- `test-java-nightly.yml`: Scheduled nightly runs for extended testing

**Key Features**:
- **SQL Compiler Testing**: Validates SQL-to-DBSP compilation pipeline
- **Extended Test Suite**: Nightly version runs more comprehensive tests
- **Maven Integration**: Uses Maven for Java build and test execution
- **Cross-platform Testing**: Tests Java components across different environments

**Test Coverage**:
- SQL parsing and validation
- DBSP code generation
- Compiler optimization passes
- Integration with Calcite framework

---

### `ci-post-release.yml` - Post-Release Tasks

**Purpose**: Executes post-release tasks after a release is published, including package publishing.

**Triggers**:
- Release events (published)

**Key Features**:
- **Package Publishing**: Coordinates publishing of Python and Rust packages
- **Post-release Automation**: Handles tasks that occur after release creation
- **Build Caching**: Uses sccache for optimized compilation during publishing
- **Parallel Publishing**: Publishes Python packages and Rust crates simultaneously

**Release Tasks**:
- Python package publishing to PyPI
- Rust crate publishing to crates.io
- Post-release validation and verification

---

### `check-failures.yml` - Failure Notification

**Purpose**: Monitors specific workflows and sends Slack notifications when they fail.

**Triggers**:
- Workflow run completion events for monitored workflows
- Specifically watches "Java SLT Nightly" and "Link Checker docs.feldera.com"

**Key Features**:
- **Selective Monitoring**: Only monitors critical workflows on main branch
- **Slack Integration**: Sends formatted notifications to Slack channels
- **Failure Detection**: Triggers on both failure and timeout conditions
- **Rich Notifications**: Includes links to failed workflow runs and repository

**Monitored Conditions**:
- Workflow failure or timeout
- Main branch executions only
- Configurable via CI_DRY_RUN variable

---

### `build-java.yml` - Java Build Workflow

**Purpose**: Compiles Java components including the SQL-to-DBSP compiler.

**Triggers**:
- Called by other workflows (workflow_call)

**Key Features**:
- **SQL Compiler Build**: Compiles the Java-based SQL-to-DBSP compiler
- **Gradle Caching**: Caches Gradle dependencies and Calcite builds
- **Apache Calcite Integration**: Handles Calcite framework dependency
- **Artifact Generation**: Produces JAR files for downstream workflows

**Build Components**:
- SQL-to-DBSP compiler
- Calcite integration components
- Maven-based Java artifacts

---

### `build-docker-dev.yml` - Development Docker Images

**Purpose**: Builds the development Docker image (feldera-dev) used by CI workflows.

**Triggers**:
- Manual workflow dispatch only

**Key Features**:
- **CI Environment**: Creates the base container image used by other workflows
- **Multi-architecture**: Builds for both AMD64 and ARM64 platforms
- **Development Tools**: Includes all tools needed for CI/CD operations
- **Registry Publishing**: Publishes to GitHub Container Registry

**Purpose**: This image contains pre-installed development dependencies and tools used across all other CI workflows, providing a consistent build environment.

---

## Development Workflow Integration

### Local Testing
- Test workflow changes in feature branches before merging
- Use `act` tool for local workflow testing where possible
- Validate Docker builds locally before pushing

### Secrets Management
- All sensitive data stored in GitHub secrets
- Environment-specific secrets for different deployment targets
- Regular rotation of authentication tokens

### Monitoring and Debugging
- Workflow run logs available in GitHub Actions tab
- Failed builds trigger notifications
- Performance metrics tracked for build times and success rates

## Documentation-Only Change Optimization

The Feldera CI/CD pipeline has been optimized to skip expensive CI jobs when only code documentation files (`CLAUDE.md` and `README.md`) are changed. This optimization significantly reduces resource consumption and provides faster feedback for code documentation updates.

### Implementation Strategy

- **`ci-pre-mergequeue.yml`** uses GitHub's native `paths-ignore` filtering:
- **`ci.yml`** uses custom conditional execution since GitHub Actions doesn't support `paths-ignore` for `merge_group` events

## Best Practices

### Workflow Development
- **Modular Design**: Break complex workflows into reusable actions
- **Conditional Logic**: Use appropriate conditions to skip unnecessary steps
- **Resource Efficiency**: Optimize for faster execution times
- **Error Handling**: Include proper error handling and cleanup

### Security Considerations
- **Least Privilege**: Use minimal required permissions
- **Secret Protection**: Never log or expose secrets in output
- **Dependency Scanning**: Regularly update action dependencies
- **Supply Chain Security**: Pin action versions to specific SHAs

### [Performance Optimization](#performance-optimization)

This documentation will be expanded with additional workflow descriptions as they are analyzed and documented.