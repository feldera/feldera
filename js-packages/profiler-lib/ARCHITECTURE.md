# Profiler Architecture

## Overview

The profiler has been split into two packages with clear separation of concerns:

```
packages/
├── profiler-lib/          # Reusable visualization library
│   ├── src/
│   │   ├── profiler.ts    # Main API class
│   │   ├── profile.ts     # Profile data parsing
│   │   ├── dataflow.ts    # Dataflow graph parsing
│   │   ├── cytograph.ts   # Cytoscape rendering
│   │   ├── selection.ts   # Data structures for selecting parts of the profile to display
│   │   ├── metadataSelection.ts # UI and data structures to select what information to display about each profile node
│   │   ├── navigator.ts   # Minimap widget
│   │   ├── util.ts        # Utilities (Option, Graph, etc.)
│   │   ├── planar.ts      # Geometry primitives
│   │   ├── zset.ts        # Z-Set implementation, used for graph diffs
│   │   └── index.ts       # Public API exports
│   └── package.json       # Library configuration
│
├── profiler-app/          # Standalone file-loading app
│   ├── src/
│   │   ├── fileLoader.ts  # File fetching and loading
│   │   └── index.ts       # Entry point
│   ├── data/              # Sample JSON files
│   ├── index.html         # HTML structure
│   └── package.json       # App configuration
│
└── web-console/           # Main web application
    └── src/lib/components/profiler/
        ├── ProfilerDiagram.svelte  # Svelte wrapper
        └── README.md               # Component documentation
```

## Design Principles

### Separation of Concerns

**profiler-lib** (Pure Visualization):
- ✅ Accepts parsed data and DOM containers
- ✅ Manages Cytoscape rendering lifecycle
- ✅ Handles user interactions (pan, zoom, hover, expand)
- ✅ No file I/O or network requests
- ✅ No global state or singletons
- ✅ Framework-agnostic (can be used in React, Vue, etc.)

**profiler-app** (File Loading):
- ✅ Provides HTML structure
- ✅ Fetches JSON files from disk
- ✅ Parses and validates data
- ✅ Error handling for I/O operations
- ✅ Thin wrapper around profiler-lib

**ProfilerDiagram.svelte** (Svelte Integration):
- ✅ Reactive lifecycle management
- ✅ Svelte 5 runes-based state
- ✅ Automatic cleanup on unmount
- ✅ Error boundary for profiler failures
- ✅ Styled layout with sidebar and graph

### API Design

The core profiler API is minimal and explicit:

```typescript
// Configuration with explicit container references
interface ProfilerConfig {
  graphContainer: HTMLElement
  selectorContainer: HTMLElement
  navigatorContainer: HTMLElement
  errorContainer?: HTMLElement
}

// Main class with clear lifecycle
class Profiler {
  constructor(config: ProfilerConfig)
  render(profile: CircuitProfile): void
  dispose(): void
  getTooltip(): HTMLElement
  reportError(message: string): void
}
```
