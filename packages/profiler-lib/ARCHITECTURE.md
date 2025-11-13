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
│   │   ├── selection.ts   # Node expansion state
│   │   ├── metadataSelection.ts # Metric/worker filtering
│   │   ├── navigator.ts   # Minimap widget
│   │   ├── util.ts        # Utilities (Option, Graph, etc.)
│   │   ├── planar.ts      # Geometry primitives
│   │   ├── zset.ts        # Z-Set for graph diffs
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

### Dependency Injection

Instead of using globals or singletons (like the old `Globals.getInstance()`), all dependencies are now injected:

**Before** (profiler monolith):
```typescript
// Globals.ts - singleton with mixed concerns
class Globals {
  static getInstance() { ... }
  tooltip: HTMLElement           // DOM management
  fetchJson() { ... }            // I/O
  loadFiles() { ... }            // App logic
  run(profile) { ... }           // Visualization
}

// Usage - implicit dependencies
Globals.getInstance().loadFiles("data", "rec")
```

**After** (clean separation):
```typescript
// profiler-lib - pure visualization
class Profiler {
  constructor(
    config: ProfilerConfig  // Explicit DOM containers
  ) { ... }

  render(profile: CircuitProfile) { ... }  // Pure rendering
}

// profiler-app - explicit I/O
class ProfileLoader {
  async loadFiles(dir, base) {
    const data = await fetch(...)  // Explicit I/O
    profiler.render(data)          // Explicit rendering
  }
}

// ProfilerDiagram.svelte - Svelte lifecycle
$effect(() => {
  profiler = new Profiler(config)  // Explicit creation
  profiler.render(profile)         // Explicit rendering
})
onDestroy(() => profiler?.dispose())  // Explicit cleanup
```

## Key Refactorings

### 1. Removed Global Singleton

**Old** (`globals.ts`):
```typescript
export class Globals {
  private static instance: Globals
  public readonly tooltip: HTMLElement

  static getInstance(): Globals { ... }
  fetchJson() { ... }
  loadFiles() { ... }
  run(profile) { ... }
}
```

**New** (`profiler.ts` in profiler-lib):
```typescript
export class Profiler {
  private readonly tooltip: HTMLElement

  constructor(config: ProfilerConfig) {
    this.tooltip = document.createElement('div')
    // ... configure tooltip
  }

  render(profile: CircuitProfile) { ... }
  dispose() { ... }
}
```

### 2. Dependency Injection for Cytograph

**Old** (`cytograph.ts`):
```typescript
constructor(
  graph: Graph<NodeId>,
  selection: CircuitSelection,
  metadataSelection: MetadataSelection
) {
  // Hardcoded DOM lookups
  let parent = document.getElementById('app')!
  this.navigator = new ViewNavigator(
    document.getElementById("navigator-parent")!
  )

  // Global tooltip access
  const globals = Globals.getInstance()
  globals.tooltip.innerHTML = ""
}
```

**New** (`cytograph.ts`):
```typescript
constructor(
  graphContainer: HTMLElement,      // Injected
  navigatorContainer: HTMLElement,  // Injected
  tooltip: HTMLElement,             // Injected
  graph: Graph<NodeId>,
  selection: CircuitSelection,
  metadataSelection: MetadataSelection
) {
  this.navigator = new ViewNavigator(navigatorContainer)
  this.cy = cytoscape({ container: graphContainer, ... })
  this.tooltip = tooltip
}
```

### 3. Separated File Loading Logic

**Old** (in `globals.ts`):
```typescript
loadFiles(directory: string, basename: string): void {
  this.fetchJson<JsonProfiles>(profileUrl)
    .then(data => {
      this.fetchJson<Dataflow>(dataflowUrl)
        .then(dfData => {
          let circuit = CircuitProfile.fromJson(data)
          circuit.setDataflow(dfData)
          this.run(circuit)  // Mixed concerns
        })
    })
}
```

**New** (in `fileLoader.ts` in profiler-app):
```typescript
async loadFiles(directory: string, basename: string): Promise<void> {
  const profileData = await this.fetchJson<JsonProfiles>(profileUrl)
  const dataflowData = await this.fetchJson<Dataflow>(dataflowUrl)

  const profile = CircuitProfile.fromJson(profileData)
  profile.setDataflow(dataflowData)

  this.profiler = new Profiler(this.config)
  this.profiler.render(profile)  // Clean separation
}
```

## Package Relationships

```
┌─────────────────┐
│  profiler-app   │  (Development tool)
│                 │
│  - Vite dev     │
│  - File loading │
│  - HTML shell   │
└────────┬────────┘
         │ depends on
         ▼
┌─────────────────┐
│  profiler-lib   │  (Core library)
│                 │
│  - Visualization│
│  - Cytoscape    │
│  - No I/O       │
└────────┬────────┘
         │ imported by
         ▼
┌─────────────────┐
│  web-console    │  (Production UI)
│                 │
│  - SvelteKit    │
│  - API client   │
│  - Auth         │
└─────────────────┘
```

## Benefits of This Architecture

### For Development
- **profiler-app** provides fast iteration on visualization features
- Sample data in `data/` directory for testing
- No need to run full web-console or pipeline manager

### For Production
- **web-console** imports clean, focused library
- No unnecessary file-loading code in production bundle
- Easy to integrate with existing API clients

### For Reusability
- **profiler-lib** can be used in:
  - Other web frameworks (React, Vue, Angular)
  - Electron apps
  - Browser extensions
  - Documentation sites

### For Testing
- Library can be tested without I/O mocking
- App can be tested without rendering complexity
- Svelte component can be tested with Playwright

## Migration Path

If you have existing code using the old monolithic profiler:

```typescript
// Old way
import { Globals } from './globals'
Globals.getInstance().loadFiles("data", "rec")

// New way (in profiler-app)
import { ProfileLoader } from './fileLoader'
const loader = new ProfileLoader(config)
await loader.loadFiles("data", "rec")

// New way (in web-console with data from API)
import ProfilerDiagram from '$lib/components/profiler/ProfilerDiagram.svelte'
<ProfilerDiagram {profileData} {dataflowData} />
```

## Future Enhancements

### Streaming Updates
Currently the profiler takes a static snapshot. Future enhancement:

```typescript
class Profiler {
  updateProfile(newData: Partial<JsonProfiles>): void {
    // Incrementally update visualization
  }
}
```

### Multiple Profiles
Compare two profiles side-by-side:

```typescript
class ProfilerComparison {
  constructor(
    leftConfig: ProfilerConfig,
    rightConfig: ProfilerConfig
  ) { ... }

  render(
    leftProfile: CircuitProfile,
    rightProfile: CircuitProfile
  ): void { ... }
}
```

### Custom Renderers
Allow customization of node/edge rendering:

```typescript
interface ProfilerConfig {
  // ... existing
  nodeRenderer?: (node: SimpleNode) => CustomNodeStyle
  edgeRenderer?: (edge: ProfileEdge) => CustomEdgeStyle
}
```
