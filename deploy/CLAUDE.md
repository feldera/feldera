# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the deploy directory and its role in Feldera's release process and CI/CD workflows.

## Overview

The `deploy/` directory contains Docker configurations and deployment orchestration for the Feldera platform. These files support multiple deployment scenarios from development builds to production releases, and integrate with the GitHub Actions workflows to provide a comprehensive CI/CD and deployment pipeline.

## Dockerfile Architecture

### **Production Runtime Image** (`Dockerfile`)

**Purpose**: Creates the production runtime container that end users download and execute.

**Release Process Role**:
- Built and published by the `build-docker.yml` workflow during releases
- Contains the pipeline manager binary and SQL compiler runtime environment
- Tagged with version numbers (e.g., `ghcr.io/feldera/pipeline-manager:v0.115.0`) by `ci-release.yml`
- Serves as the base image for customer deployments and cloud installations

**Key Characteristics**:
```dockerfile
# Multi-stage build optimized for runtime
FROM ubuntu:24.04 AS base
# Runtime dependencies: OpenJDK 21, OpenSSL, protobuf-compiler
# Optimized for production with minimal attack surface
# Supports both AMD64 and ARM64 architectures
```

**Integration Points**:
- Used in production docker-compose configurations
- Published to GitHub Container Registry by release workflows
- Referenced in quickstart documentation and deployment guides
- Base for customer self-hosted deployments

### **CI Development Environment** (`build.Dockerfile`)

**Purpose**: Standardized CI build environment used across all GitHub Actions workflows.

**Release Process Role**:
- Built by `build-docker-dev.yml` workflow when dependencies change
- Provides consistent build environment across AMD64/ARM64 runners
- Used by all compilation, testing, and validation workflows
- Eliminates "works on my machine" issues in CI/CD pipeline

**Comprehensive Toolchain**:
```dockerfile
# Complete development environment including:
# - Rust toolchain with cargo
# - Java 21 JRE + Maven for SQL compiler
# - Node.js + Bun for web console builds
# - AWS CLI for deployment testing
# - Docker for integration testing
# - Testing tools: jq, curl, git, strace
```

**Workflow Integration**:
- Referenced in 8+ workflow files with specific SHA hashes
- Updated through manual process when new dependencies are added
- Ensures reproducible builds across different runner environments
- Supports both development and release build scenarios

### **Demo Environment** (`Dockerfile.demo`)

**Purpose**: Self-contained environment for running all Feldera demos and tutorials.

**Release Process Role**:
- Validates release functionality through comprehensive demo testing
- Used in demo profiles for customer onboarding and presentations
- Ensures demos work consistently across different environments
- Part of release validation process

**Capabilities**:
```dockerfile
# Includes all demo dependencies:
# - Python client libraries and demo scripts
# - RPK (Redpanda CLI) for Kafka operations
# - SnowSQL for data warehouse integration
# - Database connectors (PostgreSQL, MySQL)
# - Fraud detection ML dependencies
```

**Demo Integration**:
- Supports 5+ different demo profiles (MySQL, PostgreSQL, Snowflake, etc.)
- Used in integration testing for release validation
- Provides consistent demo environment for customer presentations

### **Kafka Connect Integration** (`Dockerfile.kafka-connect`)

**Purpose**: Specialized Kafka Connect container with Debezium CDC and Snowflake connectors.

**Release Process Role**:
- Enables CDC (Change Data Capture) integration testing
- Validates database connector functionality in release pipeline
- Supports enterprise integration scenarios

**Enterprise Connectors**:
```dockerfile
# Based on Debezium Connect 2.7.3.Final
# Includes:
# - All Debezium CDC connectors (MySQL, PostgreSQL, etc.)
# - Snowflake Kafka Connector for data warehouse integration
# - Confluent JDBC connector for database operations
```

## Docker Compose Orchestration

### **Base Production Stack** (`docker-compose.yml`)

**Purpose**: Core production deployment configuration using released container images.

**Release Process Role**:
- Primary configuration referenced in quickstart documentation
- Uses versioned container images from GitHub Container Registry
- Provides stable deployment target for production environments
- Updated during release process with new image tags

**Core Services**:
```yaml
services:
  pipeline-manager:
    image: ghcr.io/feldera/pipeline-manager:${FELDERA_VERSION:-latest}
    # Production-ready configuration with health checks
    # CORS configuration for web console access
    # Authentication support (OAuth2/OIDC)

  redpanda:
    # Kafka-compatible streaming platform
    # Used across multiple demo profiles
    # Configured for development (single-node cluster)
```

**Profile System**:
- `redpanda`: Kafka streaming infrastructure
- `prometheus`: Metrics collection and monitoring
- `grafana`: Observability dashboards
- Multiple demo profiles for different integration scenarios

### **Development Override** (`docker-compose-dev.yml`)

**Purpose**: Overrides base configuration to build from local source code.

**Development Workflow Role**:
- Used by developers for local testing with uncommitted changes
- Referenced in development sections of deployment documentation
- Enables rapid iteration without pushing to container registry
- Critical for pre-release testing and validation

**Key Override**:
```yaml
services:
  pipeline-manager:
    build:
      context: ../
      dockerfile: deploy/Dockerfile
    # Builds fresh container from current source tree
    # Maintains all other production configuration
```

### **Extended Services** (`docker-compose-extra.yml`)

**Purpose**: Additional infrastructure services for comprehensive testing and demos.

**Integration Testing Role**:
- Provides MySQL, PostgreSQL databases for connector testing
- Kafka Connect service for CDC integration validation
- Required for database integration demos and tutorials
- Used in CI integration tests

**Infrastructure Services**:
```yaml
services:
  mysql:
    # MySQL 8.0 with CDC configuration
    # Enables Debezium change capture testing

  postgres:
    # PostgreSQL with replication configuration
    # Supports CDC and direct connector testing

  kafka-connect:
    # Debezium + Snowflake connector infrastructure
    # Enables end-to-end CDC pipeline testing
```

### **Demo Orchestration** (`docker-compose-demo.yml`)

**Purpose**: Complete demo scenarios showcasing Feldera capabilities.

**Customer Onboarding Role**:
- Provides working examples of Feldera integration patterns
- Validates end-to-end functionality in release testing
- Supports customer proof-of-concept deployments
- Demonstrates CDC, streaming analytics, and data warehouse integration

**Demo Profiles**:
```yaml
profiles:
  demo-debezium-mysql:
    # Complete CDC pipeline: MySQL → Kafka → Feldera → Analytics
    # Showcases real-time change capture and processing

  demo-supply-chain-tutorial:
    # Business-focused demo with supply chain analytics
    # Demonstrates SQL streaming analytics capabilities

  demo-snowflake-sink:
    # Data warehouse integration showcase
    # End-to-end pipeline to Snowflake data warehouse
```

## CI/CD Integration Points

### **Build Pipeline Integration**

**Image Publication Workflow**:
1. `build-docker.yml` builds production images during releases
2. Images tagged with release versions and pushed to GitHub Container Registry
3. `docker-compose.yml` updated with new version tags
4. Release artifacts published for customer consumption

**Development Environment Maintenance**:
1. `build-docker-dev.yml` rebuilds CI environment when dependencies change
2. Manual process updates workflow files with new image SHAs
3. Ensures consistent build environment across all CI workflows

### **Testing Integration**

**Multi-Stage Validation**:
- `test-integration.yml` uses docker-compose configurations for integration testing
- Demo profiles validate end-to-end functionality in release pipeline
- Database connectors tested through extended service configurations
- Multi-architecture testing (AMD64/ARM64) through specialized images

**Release Validation Process**:
1. Unit tests run in standardized CI environment (`build.Dockerfile`)
2. Integration tests validate docker-compose stack functionality
3. Demo scenarios test customer-facing capabilities
4. Production images validated before release publication

### **Release Process Integration**

**Automated Release Pipeline**:
```yaml
# ci-release.yml coordinates:
1. Build production Docker images
2. Tag with release version
3. Publish to container registry
4. Update documentation with new version tags
5. Validate through demo testing
```

**Version Management**:
- Environment variable `FELDERA_VERSION` controls image versions
- Release workflows automatically update version tags
- Production deployments can pin to specific versions
- Latest tag updated for rolling releases

## Configuration and Monitoring

### **Observability Stack**

**Prometheus Integration** (`config/prometheus.yml`):
- Scrapes metrics from pipeline manager and DBSP circuits
- Configured for development and production monitoring
- Integrated with Grafana for visualization

**Grafana Configuration** (`config/grafana_data_sources.yaml`):
- Pre-configured Prometheus data sources
- Custom dashboards for Feldera-specific metrics (`grafana_dashboard.json`)
- Production-ready observability stack

### **Development vs Production Configurations**

**Development Focus**:
- Fast iteration with local source builds
- Comprehensive logging and debugging tools
- All demo and testing infrastructure available
- Development-friendly security settings

**Production Focus**:
- Versioned, tested container images
- Security-hardened configurations
- Health checks and graceful shutdown
- Production-scale resource allocations

## Deployment Scenarios

### **Customer Self-Hosted Deployments**

**Quick Start Path**:
```bash
# Uses production images with latest stable release
docker compose -f deploy/docker-compose.yml up
```

**Development/Testing Path**:
```bash
# Builds from source for customization
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-dev.yml up --build
```

### **Enterprise Integration Scenarios**

**Database CDC Integration**:
```bash
# Complete CDC pipeline with MySQL
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-extra.yml \
               --profile mysql --profile kafka-connect up
```

**Data Warehouse Integration**:
```bash
# Snowflake integration demo
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-demo.yml \
               --profile demo-snowflake-sink up
```

## Best Practices

### **Image Management**

- Production images are multi-architecture (AMD64/ARM64)
- Version pinning for reproducible deployments
- Minimal attack surface in runtime images
- Comprehensive tooling in development images

### **Configuration Layering**

- Base configuration provides stable production foundation
- Override files enable development and testing scenarios
- Profile system allows selective service activation
- Environment variables enable runtime customization

### **Release Integration**

- All changes tested through CI before release
- Demo validation ensures customer-facing functionality works
- Version tagging provides clear upgrade paths
- Documentation automatically updated with new versions

This deployment architecture provides Feldera with a robust, scalable, and maintainable foundation for both development workflows and production deployments, while seamlessly integrating with the comprehensive CI/CD pipeline.