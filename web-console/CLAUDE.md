# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Key Development Commands

### Package Manager

This project uses **Bun** as the package manager, not npm.

```bash
# Install dependencies
bun install

# Development server
bun run dev

# Build the application
bun run build

# Code formatting and linting
bun run format     # Format code with Prettier
bun run lint       # Lint with ESLint
bun run check      # Type check with svelte-check

# Testing
bun run test-e2e      # End-to-end tests with Playwright
bun run test-e2e-ui   # E2E tests with UI
bun run test-ct       # Component tests
bun run test-ct-ui    # Component tests with UI

# OpenAPI generation (important for API updates)
bun run build-openapi     # Generate new openapi.json from Rust backend
bun run generate-openapi  # Generate TypeScript client from openapi.json

# Icon font generation
bun run build-icons
```

## Architecture Overview

### Technology Stack

- **Frontend**: SvelteKit 2.x with Svelte 5 (using runes)
- **Language**: TypeScript with strict configuration
- **Styling**: TailwindCSS + Skeleton UI components
- **Build Tool**: Vite with ESBuild minification
- **Authentication**: OIDC/OAuth2 via @axa-fr/oidc-client
- **HTTP Client**: @hey-api/client-fetch with auto-generated TypeScript bindings
- **Testing**: Playwright for E2E and component testing
- **Package Manager**: Bun (not npm)

### Project Structure

#### Core Directories

- `src/lib/components/` - Reusable Svelte components organized by feature
- `src/lib/compositions/` - Stateful functions for app state management (Svelte runes-based)
- `src/lib/functions/` - Pure functions and utilities
- `src/lib/services/` - API services and side effects (networking, storage)
- `src/lib/types/` - TypeScript type definitions
- `src/routes/` - File-based routing (SvelteKit pages)

#### Key Service Layer

- **Pipeline Manager API**: Main backend communication via `src/lib/services/pipelineManager.ts`
- **Auto-generated API client**: Located in `src/lib/services/manager/` (generated from OpenAPI spec)
- **HTTP Client Setup**: Global client configuration in `src/lib/compositions/setupHttpClient.ts`

#### Architecture Patterns

- **State Management**: Uses Svelte 5 runes (`$state`, `$derived`) via composition functions
- **Error Handling**: Centralized error reporting through toast notifications
- **Authentication**: OIDC integration with token-based API authentication
- **Real-time Features**: WebSocket/streaming connections for pipeline logs and data egress

### Important Implementation Details

#### Authentication Flow

- Uses OIDC/OAuth2 with @axa-fr/oidc-client
- Handles login redirects and session management
- Injects auth tokens into HTTP requests via interceptors
- Network health tracking for connection status

#### Pipeline Management

- Core feature: managing data processing pipelines
- Real-time status tracking and updates
- Streaming data interfaces for logs and output
- Complex state consolidation (program status vs deployment status)

#### Code Generation

- OpenAPI TypeScript client is auto-generated - do not manually edit files in `src/lib/services/manager/`
- Icon fonts are generated from SVG files - use `bun run build-icons` after adding new icons
- API schema updates require running `bun run build-openapi && bun run generate-openapi`

### Development Workflow

#### For API Changes

1. Update Rust backend API
2. Run `bun run build-openapi` to generate new OpenAPI spec
3. Run `bun run generate-openapi` to update TypeScript client
4. Update frontend code to use new API

#### For UI Components

- Use Skeleton UI components as base layer
- Follow existing component patterns in `src/lib/components/`
- Use Svelte 5 runes syntax (`$state`, `$derived`, `$effect`)
- Implement proper TypeScript typing

#### Testing Strategy

- E2E tests for critical user workflows
- Component tests for isolated UI testing
- Use Playwright for both E2E and component testing
- Tests expect pipeline manager backend to be running on localhost:8080

### Configuration Files

- `svelte.config.js` - SvelteKit configuration with static adapter
- `vite.config.ts` - Build configuration with plugins for SVG and virtual modules
- `tailwind.config.ts` - Custom theme configuration with Skeleton integration
- `eslint.config.js` - Flat config with TypeScript and Svelte support
- `playwright.config.ts` - Test configuration
