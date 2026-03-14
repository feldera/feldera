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

# Run unit tests
bun run test

# OpenAPI generation (important for API updates)
bun run build-openapi     # Generate new openapi.json from Rust backend
bun run generate-openapi  # Generate TypeScript client from openapi.json

# Icon packs webfont generation
bun run build-icons
```

## Architecture Overview

Web Console is a dasboard UI app deployed as a static website that is served by the pipeline-manager service in a Feldera deployment. The app is self-contained and does not need an internet connection to function properly.

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
- `src/lib/compositions/` - Stateful functions for app state management (Svelte runes-based), organized by feature family where applicable
- `src/lib/functions/` - Pure functions and utilities, organized by application
- `src/lib/services/` - API services and side effects (networking, storage)
- `src/lib/types/` - TypeScript type definitions
- `src/routes/` - File-based routing (SvelteKit pages)

#### Key Service Layer

- **Pipeline Manager API**: Main backend communication via `src/lib/services/pipelineManager.ts` which wraps the auto-generated API client
- **Auto-generated API client**: Located in `src/lib/services/manager/` (generated from OpenAPI spec)
- **HTTP Client Setup**: Global client configuration in `src/lib/compositions/setupHttpClient.ts`

#### Architecture Patterns

- **State Management**: Uses Svelte 5 runes (`$state`, `$derived`) via composition functions
- **Error Handling**: Centralized request error and occasional client error reporting through toast notifications
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

## Testing Strategy

- **vitest** for unit tests and UI component tests (vitest-browser-svelte renders Svelte components in a real browser via Playwright)
- **Playwright** for e2e tests (expect pipeline-manager backend running on localhost:8080)
- Snapshots (visual screenshots) are stored in `playwright-snapshots/` which is gitignored — they live in a separate repo

### Test Commands

- `bun run test` — run vitest unit + component tests (single run)
- `bun run test-unit` — run vitest in watch mode
- `bun run test-e2e` — run Playwright e2e tests

### Snapshot Workflow

1. `bun run test-prepare` — clones the snapshot repo (or creates empty dirs)
2. `bun run test` — vitest reads/writes component snapshots from `playwright-snapshots/component/`
3. `bun run test-e2e` — Playwright reads/writes e2e snapshots from `playwright-snapshots/e2e/`
4. `bun run test-update-snapshots` — regenerates all snapshots

### Test ID Conventions

Svelte markup uses `data-testid` attributes: `[btn|input|box]-[element id]`

- **btn** — any clickable element (`<button/>`, `<a/>`, element with `onclick={}`)
- **input** — any other input element
- **box** — any other element or area
- Element id should be concise but not abbreviated

Most interactions and checks in the tests should be performed through data-testid attributes. The exception is locating an icon based on it's webfont class name (e.g. fd-circle-alert)

## Configuration Files

- `svelte.config.js` - SvelteKit configuration with static adapter
- `vite.config.ts` - Build configuration with plugins for SVG and virtual modules
- `tailwind.config.ts` - Custom theme configuration with Skeleton integration
- `eslint.config.js` - Flat config with TypeScript and Svelte support
- `playwright.config.ts` - e2e test configuration
