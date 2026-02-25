# profiler-layout

## Overview

**profiler-layout** is a reusable Svelte component library for visualizing Feldera DBSP circuit profiles. It provides shared UI components and utilities for displaying incremental computation performance metrics, circuit graphs, and profiling data across multiple applications.

This package serves as the presentation layer between profiler-lib (the core visualization engine) and consuming applications (web-console, profiler-app).

## Purpose

Enable consistent profiler visualization across the Feldera ecosystem:
- **Reusable Components**: Shared UI components for profile visualization
- **Bundle Processing**: Common utilities for extracting and parsing profile data from support bundles
- **Consistent UX**: Unified user experience across web-console and standalone profiler-app
- **Separation of Concerns**: Decouples visualization logic from application-specific state management

## Architecture

### Component-Based Design

This library follows Svelte 5 patterns with runes for reactive state management. Components are designed to be composable and accept data through props while exposing bindable properties for two-way communication.

### Key Components

#### **ProfilerLayout**
The main layout component orchestrating the complete profiler visualization experience:
- Combines diagram, tooltip, and control elements
- Accepts profile data, dataflow graph, and program source code
- Provides toolbar slots for application-specific controls
- Handles layout and styling coordination

#### **ProfilerDiagram**
Wraps profiler-lib's Cytoscape-based circuit graph visualization:
- Renders DBSP operator graphs with performance metrics
- Manages interaction states and visual feedback
- Integrates with profiler-lib's callback-based API

#### **ProfilerTooltip**
Displays detailed information about selected circuit nodes:
- Shows operator metrics, source code references, and performance data
- Positioned relative to diagram interactions
- Receives structured data through profiler-lib callbacks

#### **ProfileTimestampSelector**
Reusable dropdown for switching between profile snapshots:
- Accepts array of timestamp/files pairs
- Exports bindable selectedTimestamp property
- Conditionally renders when multiple profiles are available

### Utility Functions

#### **Bundle Processing** (`functions/processZipBundle.ts`)

##### `getSuitableProfiles(zipData: Uint8Array): [Date, ZipItem[]][]`
Unzips support bundle once and returns all valid profile timestamps with their associated files. Critical for efficient timestamp switching - ensures unzip only happens once per bundle upload.

##### `processProfileFiles(files: ZipItem[]): Promise<ProcessedProfile>`
Processes already-unzipped profile files into structured data. Works with files from getSuitableProfiles to avoid redundant unzipping.

**Workflow Pattern:**
1. Call getSuitableProfiles once when bundle is uploaded
2. Store the returned [Date, ZipItem[]][] array
3. When user switches timestamps, call processProfileFiles with the selected files
4. No re-unzipping occurs - efficient timestamp navigation

## Development Workflow

### Package Manager
This project uses **Bun** as the package manager.

### Key Commands
- `bun install` - Install dependencies
- `bun run dev` - Development server
- `bun run build` - Build library (also builds profiler-lib dependency)
- `bun run check` - Type checking with svelte-check
- `bun run prepack` - Package for distribution

### Build Process
The prebuild script automatically rebuilds profiler-lib before building this package, ensuring the latest profiler-lib changes are included.

### Testing Strategy
Components are tested through integration in consuming applications:
- **profiler-app**: Standalone testing with manual bundle uploads
- **web-console**: Integration testing with live pipeline data

## Dependencies

### Runtime Dependencies
- **but-unzip**: Browser-based zip extraction for processing support bundles
- **sort-on**: Utility for sorting profile timestamps

### Peer Dependencies
- **svelte ^5.0.0**: Component framework (runes-based reactivity)

### Development Dependencies
- **profiler-lib**: Core visualization engine (workspace dependency)
- **@skeletonlabs/skeleton-svelte**: UI component library for consistent styling
- **SvelteKit**: Build tooling and package publishing

## Integration Points

### web-console
Integrated into TabProfileVisualizer for live pipeline profiling:
- Downloads support bundles from pipeline-manager API
- Allows timestamp selection for multi-snapshot bundles
- Integrates with web-console's authentication and error handling

### profiler-app
Standalone single-page application for offline profile viewing:
- File picker for manual bundle uploads
- Self-contained profiler without backend dependencies
- Builds to single HTML file for distribution

## Key Patterns

### State Management
Components use Svelte 5 runes ($state, $derived, $props) for reactivity. Parent applications manage application-specific state while components handle presentation logic.

### Two-Way Binding
Components expose bindable properties (using $bindable) to allow parent applications to control and react to state changes without prop drilling.

### Slot-Based Composition
ProfilerLayout uses Svelte snippets for toolbar customization, allowing applications to inject their own controls while maintaining consistent layout.

### Incremental Computation Awareness
Components are designed around DBSP's incremental computation model - profile data represents snapshots of circuit execution state at specific timestamps.

## Important Notes

### Bundle Format Requirements
Support bundles must contain three required files per profile timestamp:
- `{timestamp}_circuit_profile.json` - DBSP circuit performance metrics
- `{timestamp}_dataflow_graph.json` - Circuit structure and operator graph
- `{timestamp}_pipeline_config.json` - SQL source code and configuration

### Timestamp Extraction
Profile timestamps are extracted from filename prefixes using regex pattern `/^(.*?)_/`. Files without matching prefixes or incomplete file sets are filtered out.

### Build Output
Package is published as ESM module with TypeScript definitions. The `dist/` directory contains compiled components and utilities ready for consumption by other packages.

### Styling Dependencies
Components assume Skeleton UI classes are available in the consuming application. Both web-console and profiler-app include the necessary Skeleton UI setup.

## Related Documentation

For comprehensive repository context and IVM architecture, see `/workspaces/feldera/CLAUDE.md`.

For profiler-lib internals and visualization engine details, see `/workspaces/feldera/js-packages/profiler-lib/CLAUDE.md` (when created).

For web-console integration patterns, see `/workspaces/feldera/js-packages/web-console/CLAUDE.md`.

For profiler-app standalone usage, see `/workspaces/feldera/js-packages/profiler-app/CLAUDE.md`.