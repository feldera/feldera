# profiler-app

## Overview

**profiler-app** is a standalone single-page application for visualizing Feldera DBSP circuit profiles. It builds to a single static HTML file that can load profile bundles (`.zip` files) through a browser file picker UI.

This is a **development/debugging tool**, not intended for production use. In production, use the web-console's integrated profiler with real-time API data.

## Purpose

Have a lightweight, standalone profile visualizer that can be used independently of web-console
and be distributed as a part of the support bundle.

## Architecture

### Single Static HTML Page

profiler-app builds to a completely self-contained HTML file with all JavaScript and CSS inlined. The built file can be:
- Opened directly in a browser (`file:///path/to/index.html`)
- Served by any static web server
- Shared via email, cloud storage, or version control

### Technology Stack

- **Framework**: Svelte 5 with runes-based reactivity
- **Build Tool**: Vite with single-file plugin
- **Styling**: TailwindCSS with Skeleton UI components

### What It Does

1. **Bundle processing** - Extract profiles from support bundles in the browser
2. **Visualization** - Render circuit profiles
3. **File picker UI** - Manual bundle selection through "Load Bundle" button
4. **Timestamp selection** - Switch between multiple profile snapshots
5. **Error handling** - Display loading and parsing errors

## Module Structure

```
profiler-app/
├── src/
│   ├── main.ts              # Entry point - mounts Svelte app
│   ├── App.svelte           # Main component with state management
│   ├── app.css              # Global styles and theme imports
│   ├── app.d.ts             # TypeScript definitions
│   └── assets/              # Fonts and static assets
├── index.html               # HTML shell
├── package.json             # Dependencies and build scripts
├── vite.config.ts           # Vite config with singlefile plugin
├── svelte.config.js         # Svelte configuration
└── tsconfig.json            # TypeScript configuration
```

## Key Components

### Entry Point (`main.ts`)

Minimal entry point that mounts the Svelte application:
- Uses Svelte 5's `mount()` API
- Targets the `#app` div in index.html
- Imports global styles

### Main Application (`App.svelte`)

Single-component application managing all state and UI with Svelte 5 runes:

**UI Structure:**
- Welcome screen with upload instructions (shown when no profile loaded)
- ProfilerLayout with visualization (shown when profile loaded)
- ProfileTimestampSelector for switching between snapshots
- Hidden file input for bundle selection
- Error and loading state displays

### HTML Shell (`index.html`)

Minimal HTML template:
- Sets Feldera Modern theme via data attribute
- Provides mounting point for Svelte app
- Vite injects all assets during build

## Build Configuration

### Vite Configuration (`vite.config.ts`)

Configured to produce a single HTML file with all assets inlined:
- **Plugins**: TailwindCSS, Svelte, SVG component loader, single-file bundler
- **Aliases**: $assets for asset imports
- **Server**: Runs on port 5174
- **Build**: Target esnext with full asset inlining

**Key Configuration:**
- `viteSingleFile()` - Inlines all JS/CSS into HTML
- `assetsInlineLimit: 100000000` - Forces inlining of all assets
- `cssCodeSplit: false` - Prevents CSS splitting
- `inlineDynamicImports: true` - Inlines all dynamic imports

### Monorepo Dependencies

- `profiler-layout` - Reusable profiler components and utilities
- `profiler-lib` - Core visualization engine

## Key Patterns

### Efficient Bundle Processing

Follows the unzip-once pattern from profiler-layout:
1. Upload triggers `getSuitableProfiles(zipData)` - unzips once
2. Store [Date, ZipItem[]][] pairs in state
3. User switches timestamps via ProfileTimestampSelector
4. `$effect()` watches selectedTimestamp and calls `processProfileFiles()`
5. No re-unzipping - just reads from cached ZipItem array

### Reactive State Management

Uses Svelte 5 runes for clean reactivity:
- `$state()` - Reactive state variables
- `$effect()` - Side effects that run when dependencies change
- `$derived()` - Computed values (used in child components)
- `bind:` - Two-way binding with ProfileTimestampSelector

### Component Composition

Leverages profiler-layout components:
- **ProfilerLayout** - Main visualization container
- **ProfileTimestampSelector** - Timestamp dropdown with bindable selection
- Uses snippets (`{#snippet}`) for toolbar customization

## Development Workflow

### Package Manager
This project uses **Bun** as the package manager.

### Key Commands
- `bun install` - Install dependencies
- `bun run dev` - Development server on port 5174
- `bun run build` - Build to single HTML file in `dist/`
- `bun run preview` - Preview production build
- `bun run check` - Type checking

### Build Output
Produces `dist/index.html` - a single self-contained HTML file with all dependencies inlined.

### Testing Strategy
Manual testing with support bundle uploads. Use sample bundles from:
- Feldera pipeline support bundle downloads
- Saved bundles from previous profiler sessions

## Bundle Format Requirements

Support bundles must contain three required files per profile timestamp:
- `{timestamp}_circuit_profile.json` - DBSP circuit performance metrics
- `{timestamp}_dataflow_graph.json` - Circuit structure and operator graph
- `{timestamp}_pipeline_config.json` - SQL source code and configuration

Multiple timestamps can exist in a single bundle, enabling snapshot comparison.

## Integration with profiler-layout

This app is a thin wrapper around profiler-layout components:
- Uses `getSuitableProfiles()` for bundle extraction
- Uses `processProfileFiles()` for profile parsing
- Uses `ProfilerLayout` for visualization
- Uses `ProfileTimestampSelector` for UI controls

Changes to profiler-layout automatically propagate to profiler-app on next build.

## Distribution

**Build Process:**
1. Run `bun run build` to create `dist/index.html`
2. Single HTML file contains all code, styles, and assets
3. File can be opened directly in any modern browser

**Sharing Profiles:**
1. Download support bundle from Feldera pipeline
2. Share `index.html` + bundle with team members
3. Recipients open HTML and upload bundle locally
4. No installation or server required

## Development Flow

**Feature Development:**
1. Develop visualization features in profiler-lib
2. Create reusable components in profiler-layout
3. Test quickly in profiler-app with sample bundles
4. Integrate into web-console for production use
5. Deploy web-console to users

**Debugging:**
1. Export support bundle from failing pipeline
2. Load in profiler-app for offline analysis
3. Iterate on visualization without backend connection
4. Share findings via bundled HTML file

## Styling

Uses the Feldera Modern theme with:
- **Primary color**: Purple gradient branding
- **Font**: DM Sans for UI, DM Mono for code
- **Components**: Skeleton UI for buttons, inputs, and containers
- **Layout**: TailwindCSS utilities for responsive design

Theme is applied via `data-theme="feldera-modern-theme"` on the html element.

## Limitations

- No SQL compilation support (requires pre-compiled dataflow graphs)
- Browser-only - cannot access filesystem or make HTTP requests to Feldera instance
- Single-user tool - no collaboration features
- No persistent storage - reload requires re-uploading bundle

## Related Documentation

For profiler-layout component details, see `/workspaces/feldera/js-packages/profiler-layout/CLAUDE.md`.

For web-console integration, see `/workspaces/feldera/js-packages/web-console/CLAUDE.md`.

For repository overview, see `/workspaces/feldera/CLAUDE.md`.