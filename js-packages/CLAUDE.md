# js-packages

## Overview

The `js-packages/` directory contains TypeScript/JavaScript packages for the Feldera platform's user-facing interfaces and visualization tools. These packages form the frontend ecosystem for interacting with Feldera's incremental view maintenance system.

All packages use **Bun** as the package manager and are organized as a monorepo workspace.

## Package Organization

### **web-console**
Production web application for managing Feldera pipelines and data processing workflows.

**Purpose**: Complete dashboard UI for pipeline management, SQL editing, real-time monitoring, and profiling
**Technology**: SvelteKit 2.x with Svelte 5, TailwindCSS, Skeleton UI
**Deployment**: Static site served by pipeline-manager service
**Key Features**:
- Pipeline lifecycle management (create, configure, start, stop)
- SQL editor with Monaco integration
- Real-time metrics and performance monitoring
- Connector configuration for data sources/sinks
- Integrated profiler with circuit visualization
- OIDC/OAuth2 authentication

See `web-console/CLAUDE.md` for detailed documentation.

### **profiler-app**
Standalone single-page application for offline profile visualization.

**Purpose**: Development tool for analyzing DBSP circuit profiles without backend dependencies
**Technology**: Svelte 5 with Vite single-file plugin
**Deployment**: Single self-contained HTML file
**Key Features**:
- Browser-based support bundle upload
- Profile timestamp selection
- Offline circuit visualization
- Shareable HTML output for team collaboration

See `profiler-app/CLAUDE.md` for detailed documentation.

### **profiler-layout**
Reusable Svelte component library for circuit profile visualization.

**Purpose**: Shared UI components and utilities for profiler features across web-console and profiler-app
**Technology**: Svelte 5 component library with SvelteKit packaging
**Deployment**: NPM workspace package
**Key Components**:
- `ProfilerLayout` - Main visualization container
- `ProfilerDiagram` - Circuit graph renderer
- `ProfilerTooltip` - Node detail display
- `ProfileTimestampSelector` - Timestamp navigation
**Key Utilities**:
- `getSuitableProfiles()` - Efficient zip bundle extraction
- `processProfileFiles()` - Profile data parsing

See `profiler-layout/CLAUDE.md` for detailed documentation.

### **profiler-lib**
Core visualization engine for DBSP circuit profiling.

**Purpose**: Low-level profiler implementation with Cytoscape graph visualization
**Technology**: TypeScript with Cytoscape.js and ELK layout
**Deployment**: NPM workspace package
**Key Features**:
- Callback-based API for UI integration
- Incremental circuit graph rendering
- Performance metric visualization
- Worker-based filtering
- Node search and highlighting

See `profiler-lib/CLAUDE.md` for detailed documentation.

### **feldera-theme**
CSS theme package for consistent branding across applications.

**Purpose**: Reusable Feldera Modern theme CSS
**Technology**: Pure CSS theme configuration
**Deployment**: NPM workspace package
**Contents**: Skeleton UI theme customization with Feldera branding (purple gradients, DM Sans fonts)

## Monorepo Dependency Graph

```
web-console
├── profiler-layout
├── feldera-theme
└── ...

profiler-app
├── profiler-layout
├── feldera-theme
└── ...

profiler-layout
├── profiler-lib
└── ...

profiler-lib
└── [Cytoscape, ELK]
```

## Key Architectural Patterns

### **Incremental Computation Awareness**
All packages are designed around DBSP's incremental computation model:
- Profile data represents circuit execution snapshots at specific timestamps
- Visualization focuses on operator performance and change propagation
- UI supports comparing profiles across time

### **Component Reusability**
profiler-layout serves as the presentation layer between profiler-lib (engine) and applications (web-console, profiler-app):
- Prevents code duplication
- Ensures consistent UX across applications
- Allows independent testing and development

### **Efficient Bundle Processing**
Shared unzip-once pattern across consuming applications:
1. Call `getSuitableProfiles(zipData)` once when bundle is uploaded
2. Store [Date, ZipItem[]][] array in application state
3. Switch timestamps by calling `processProfileFiles()` with cached files
4. No redundant unzipping - efficient navigation

### **Svelte 5 Runes**
All Svelte packages use Svelte 5's runes-based reactivity:
- `$state()` - Reactive state variables
- `$derived()` - Computed values
- `$effect()` - Side effects that run when dependencies change
- `$bindable` - Two-way binding for component props

## Development Workflow

### **Package Manager**
All packages use **Bun** as the package manager.

### **Workspace Commands**
From repository root:
- `bun install` - Install dependencies for all packages
- `bun run -r build` - Build all packages
- `bun run -r check` - Type check all packages

### **Package-Specific Commands**
Navigate to individual package directories and run:
- `bun install` - Install package dependencies
- `bun run dev` - Development server
- `bun run build` - Build package
- `bun run check` - Type checking

### **Build Dependencies**
Some packages have build-time dependencies:
- **profiler-layout** builds profiler-lib before its own build (prebuild script)
- **web-console** requires profiler-layout to be built first
- **profiler-app** requires profiler-layout to be built first

## Shared Technologies

### **UI Framework**
- **Svelte 5**: Component framework with runes-based reactivity
- **SvelteKit**: Build tooling and app framework (web-console)
- **Vite**: Build tool and dev server

### **Styling**
- **TailwindCSS**: Utility-first CSS framework
- **Skeleton UI**: Component library for consistent design
- **feldera-theme**: Custom Feldera branding

### **TypeScript**
All packages use TypeScript with strict type checking enabled.

### **Profiler Stack**
- **profiler-lib**: Core visualization engine (Cytoscape + ELK)
- **profiler-layout**: Reusable UI components
- **but-unzip**: Browser-based zip extraction

## Integration Points

### **web-console ↔ pipeline-manager**
- OpenAPI-generated TypeScript client for type-safe API communication
- WebSocket connections for real-time logs and metrics
- Support bundle downloads for profiler integration

### **profiler-app ↔ filesystem**
- Browser file picker for manual bundle uploads
- No server communication - fully offline operation

### **profiler-layout ↔ applications**
- Svelte component composition via props and snippets
- Two-way binding for interactive controls
- Shared utilities for bundle processing

## Testing Strategy

### **web-console**
- End-to-end tests with Playwright
- Component tests for isolated UI validation
- Integration tests against local pipeline-manager

### **profiler-app**
- Manual testing with support bundles
- Visual regression testing of visualizations

### **profiler-layout**
- Integration testing through consuming applications
- Component composition testing

### **profiler-lib**
- Unit tests for core visualization logic
- Integration tests with mock data

## Build Outputs

### **web-console**
Static site output in `build/`:
- Client-side rendered pages
- Optimized assets (JS, CSS, images)
- Served as static files by pipeline-manager in production

### **profiler-app**
Single HTML file in `dist/`:
- All JavaScript, CSS, and assets inlined
- Self-contained for offline use
- Can be opened directly in browser

### **profiler-layout**
Library package in `dist/`:
- ESM module with TypeScript definitions
- Svelte components for consumption by other packages

### **profiler-lib**
Library package in `dist/`:
- Compiled TypeScript
- Type definitions for TypeScript consumers

### **feldera-theme**
CSS file:
- `feldera-modern.css` - Theme stylesheet

## Related Documentation

For comprehensive repository context and IVM architecture, see `feldera/CLAUDE.md`.

For package-specific details, see individual `CLAUDE.md` files in each package directory.