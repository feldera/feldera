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
bun run test-unit

# OpenAPI generation (important for API updates)
bun run build-openapi     # Generate new openapi.json from Rust backend
bun run generate-openapi  # Generate TypeScript client from openapi.json

# Icon CSS (Lucide `fd-*` classes) and generic icon webfont generation
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
- Icons: `<span class="fd fd-<name>">` shows the Lucide icon `<name>` (from `lucide-static`), sized by `font-size` and coloured by `color`. `scripts/build-lucide-icons.ts` writes `src/assets/icons/fd.css` for every `fd-<name>` it finds in the sources; run `bun run build-icons` after you use a new one. Set the stroke width in `STROKE_WIDTH` there. Icons that Lucide does not have go in `src/assets/icons/custom/`. The `gc-*` classes are still a webfont built from `src/assets/icons/generic/`
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

- `bun run test-unit` — run vitest unit + component tests (single run)
- `bun run test-e2e` — run Playwright e2e tests (all specs under `tests/*.e2e.ts`)

Run a single e2e test (args go after `--`, e2e expects pipeline-manager on `localhost:8080`):

- `bun run test-e2e -- tests/pipelineDeleted.e2e.ts` — one spec file
- `bun run test-e2e -- -g "shows Deleted chip"` — by regex against describe + title
- `bun run test-e2e -- tests/pipelineDeleted.e2e.ts:42` — single test at a file:line

### Snapshot Workflow

`playwright-snapshots.commit` pins the commit of https://github.com/feldera/playwright-snapshots that the tests use. `scripts/playwright-snapshots.ts` downloads and publishes it.

1. `bun run test-prepare` — downloads the pinned commit to `playwright-snapshots/`. `test-unit`, `test-e2e` and `test-update-snapshots` run it first. It skips the download when the folder already holds that commit, and refuses to replace screenshots that are not pushed.
2. `bun run test-unit` — vitest reads/writes component snapshots from `playwright-snapshots/component/`
3. `bun run test-e2e` — Playwright reads/writes e2e snapshots from `playwright-snapshots/e2e/`
4. `bun run test-update-snapshots` — regenerates all snapshots
5. `bun run test-snapshots-push` — commits `playwright-snapshots/` as the full contents of the snapshot repo, pushes it over SSH, and pins the new commit. Commit `playwright-snapshots.commit` with the change that needs the new screenshots.

Gotcha: `toMatchScreenshot` captures at 1:1 only while the test viewport (`page.viewport()`) fits in the 1920x1080 browser window of `vite.config.ts`.

### Test ID Conventions

Svelte markup uses `data-testid` attributes: `[btn|input|box]-[element id]`

- **btn** — any clickable element (`<button/>`, `<a/>`, element with `onclick={}`)
- **input** — any other input element
- **box** — any other element or area
- Element id should be concise but not abbreviated

Most interactions and checks in the tests should be performed through data-testid attributes. The exception is locating an icon by its class name (e.g. fd-circle-alert)

## Configuration Files

- `svelte.config.js` - SvelteKit configuration with static adapter
- `vite.config.ts` - Build configuration with plugins for SVG and virtual modules
- `tailwind.config.ts` - Custom theme configuration with Skeleton integration
- `eslint.config.js` - Flat config with TypeScript and Svelte support
- `playwright.config.ts` - e2e test configuration
