# CLAUDE.md - profiler-lib

## Overview

**profiler-lib** is a framework-agnostic TypeScript library for visualizing Feldera DBSP circuit profiles. It provides interactive graph-based visualization using Cytoscape.js with hierarchical layout, enabling performance analysis, bottleneck identification, and SQL source attribution for compiled pipelines.

This is a **pure visualization library** with no I/O, no global state, and explicit dependency injection. It can be embedded in any web framework (Svelte, React, Vue, Angular) or standalone applications.

## Purpose

Transform raw profile data into interactive visualizations that help developers:
- Identify performance hotspots through color-coded nodes
- Navigate hierarchical circuit structures with expand/collapse
- Trace data flow through forward/backward edge highlighting
- Inspect per-operator metrics across multiple worker threads
- Map operators back to originating SQL source code

## Architecture

### Clean Separation of Concerns

This library handles **only visualization and calculation**:
- ✅ Calculates graph layout and renders diagram using Cytoscape
- ✅ Computes metric visualizations and tooltip data
- ✅ Handles user interactions (pan, zoom, hover, double-click)
- ✅ Communicates UI state changes via callbacks
- ✅ No file I/O, no network requests, no global singletons
- ✅ No UI control rendering or DOM manipulation outside graph containers

What it does **not** do:
- ❌ Fetch data from files or APIs (caller's responsibility)
- ❌ Render UI controls (metric selectors, worker checkboxes, tooltips)
- ❌ Manage authentication or authorization
- ❌ Handle routing or navigation

### Public API Surface

The library exports a minimal, callback-based API via `src/index.ts`:

```typescript
// Main profiler class and configuration
export { Profiler, type ProfilerConfig, type ProfilerCallbacks } from './profiler.js'

// UI state types
export { type MetricOption, type WorkerOption } from './profiler.js'

// Tooltip data types
export { type TooltipData, type TooltipRow, type TooltipCell } from './profiler.js'

// Data parsing types
export { CircuitProfile, type JsonProfiles } from './profile.js'
export { type Dataflow } from './dataflow.js'
```

### Core Module Responsibilities

**`profiler.ts`** - Main API Class (~190 lines)
- Primary entry point for library users
- Accepts `ProfilerConfig` with graph/navigator containers and `ProfilerCallbacks`
- Orchestrates initialization of selectors, graph, and rendering
- Manages profiler lifecycle (create, render, dispose)
- Exports public methods for interaction: `selectMetric()`, `toggleWorker()`, `toggleAllWorkers()`, `search()`
- Defines callback interfaces and UI state types
- Key types: `ProfilerCallbacks`, `TooltipData`, `MetricOption`, `WorkerOption`

**`profile.ts`** - Profile Data Model (787 lines)
- Parses `JsonProfiles` from Feldera pipeline manager
- Decodes hierarchical circuit structure (simple nodes, clusters, edges)
- Processes worker-level measurements (time, memory, cache stats)
- Handles multiple property value types: `TimeValue`, `NumberValue`, `PercentValue`, `StringValue`
- Merges artificially split Z^-1 (delay/trace) nodes
- Computes aggregate measurements for complex nodes from children
- Integrates SQL source positions via persistent node IDs
- Key classes: `CircuitProfile`, `SimpleNode`, `ComplexNode`, `ProfileEdge`, `Measurement`

**`dataflow.ts`** - Dataflow Graph Model (151 lines)
- Parses dataflow JSON from SQL compiler (`--dataflow` flag)
- Represents MIR (Mid-level IR) node operations
- Maps source code positions (line/column ranges) to circuit operators
- Extracts SQL source fragments with highlighting for debugging
- Key classes: `Dataflow`, `MirNode`, `Sources`, `SourcePositionRange`

**`cytograph.ts`** - Cytoscape Rendering (~900 lines)
- Converts `CircuitProfile` to Cytoscape-compatible graph structure
- Manages node expansion/collapse for hierarchical circuits
- Computes incremental graph diffs using `ZSet` for efficient updates
- Handles edge rewriting when nodes are collapsed into parent containers
- Renders performance metrics as node colors (percentile-based heatmap)
- Builds structured tooltip data on hover with per-worker heatmap rows (sorted alphabetically), source code, and attributes
- Highlights reachable edges on hover (forward paths in red, backward in blue)
- Uses ELK hierarchical layout algorithm for high-quality graph positioning
- Communicates tooltip updates via callbacks with structured data
- Key classes: `Cytograph`, `CytographRendering`, `GraphNode`, `GraphEdge`

**`selection.ts`** - Node Expansion State (60 lines)
- Manages which circuit regions are expanded vs collapsed
- Tracks user's expand/collapse actions via double-click
- Auto-expands all regions for small circuits (<100 nodes)
- Triggers graph recomputation when selection changes
- Key classes: `CircuitSelector`, `CircuitSelection`

**`metadataSelection.ts`** - Metric/Worker State Management (~120 lines)
- Manages which performance metrics drive node coloring
- Tracks worker visibility state
- Notifies UI via callbacks when metrics or workers change
- Provides methods for metric selection and worker toggling
- Key classes: `MetadataSelector`, `MetadataSelection`

**`navigator.ts`** - Minimap Widget (84 lines)
- Displays minimap showing current viewport within full graph
- Renders two nested rectangles (graph bounds and visible viewport)
- Scales dynamically as user pans/zooms main graph
- Double-click to fit entire graph in viewport
- Key class: `ViewNavigator`

**Utility Modules:**

**`util.ts`** (455 lines)
- `Option<T>`: Rust-style nullable type with safe unwrapping
- `OMap<K,V>`: Map wrapper returning `Option` for lookups
- `Graph<T>`: Directed graph with DFS, reachability queries, depth computation
- `NumericRange`: Min/max bounds with percentile/quantile calculations
- `SubList`/`SubSet`: Filter collections by index/membership predicates
- `Edge<T>`: Graph edge with weight and backedge flag
- Assertion utilities: `assert()`, `fail()`, `enforceNumber()`

**`planar.ts`** (119 lines)
- Geometric primitives for navigator minimap rendering
- `Point`: 2D coordinates with min/max/scale operations
- `Size`: Positive width/height dimensions
- `Rectangle`: Axis-aligned boxes with intersection/bounding box

**`zset.ts`** (103 lines)
- Z-Set (multiset with positive/negative weights) for graph diffs
- Computes incremental changes between graph states
- Enables efficient Cytoscape updates (add/remove minimal nodes/edges)
- Maps objects to weights via string encoding

**`cytoscape-elk.d.ts`** (3 lines)
- TypeScript module declaration for `cytoscape-elk` plugin

## Usage Pattern

### Basic Usage with Callbacks

```typescript
import {
  Profiler,
  CircuitProfile,
  type ProfilerConfig,
  type ProfilerCallbacks,
  type TooltipData,
  type JsonProfiles,
  type Dataflow
} from 'profiler-lib'

// 1. Obtain profile and dataflow data
const profileData: JsonProfiles = await fetchProfileFromAPI()
const dataflowData: Dataflow = await fetchDataflowFromAPI()

// 2. Parse the data
const profile = CircuitProfile.fromJson(profileData)
profile.setDataflow(dataflowData, programCode)

// 3. Define callbacks for UI updates
const callbacks: ProfilerCallbacks = {
  onTooltipUpdate: (data: TooltipData | null, visible: boolean) => {
    // Render tooltip with data.columns, data.rows, data.sources, data.attributes
    updateTooltipUI(data, visible)
  },
  onMetricsChanged: (metrics, selectedMetricId) => {
    // Populate metric dropdown with available metrics
    renderMetricSelector(metrics, selectedMetricId)
  },
  onWorkersChanged: (workers) => {
    // Render worker checkboxes with checked state
    renderWorkerCheckboxes(workers)
  },
  onMessage: (msg) => console.log(msg),
  onMessageClear: () => {},
  onError: (err) => console.error(err)
}

// 4. Set up configuration
const config: ProfilerConfig = {
  graphContainer: document.getElementById('graph')!,
  navigatorContainer: document.getElementById('minimap')!,
  callbacks
}

// 5. Create profiler and render
const profiler = new Profiler(config)
profiler.render(profile)

// 6. Interact with profiler via public methods
profiler.selectMetric('time')
profiler.toggleWorker('0')
profiler.search('node-123')

// 7. Clean up when done
profiler.dispose()
```

### Required HTML Structure

```html
<!-- Graph container -->
<div id="graph" style="width: 100%; height: 80vh;"></div>

<!-- Navigator minimap -->
<div id="minimap" style="width: 108px; height: 108px;"></div>

<!-- UI controls (managed by your application, not profiler-lib) -->
<select id="metric-selector"></select>
<div id="worker-checkboxes"></div>
<button id="toggle-workers">Toggle All</button>
<input id="search" placeholder="Node ID" />

<!-- Tooltip container (managed by your application) -->
<div id="tooltip"></div>
```

### Callback-Based Architecture

The library uses callbacks to communicate UI state changes to the parent application:

**`ProfilerCallbacks` Interface:**
```typescript
interface ProfilerCallbacks {
  // Tooltip data updates (null = hide tooltip)
  onTooltipUpdate: (data: TooltipData | null, visible: boolean) => void

  // Available metrics changed (initialize dropdown)
  onMetricsChanged: (metrics: MetricOption[], selectedMetricId: string) => void

  // Worker state changed (update checkboxes)
  onWorkersChanged: (workers: WorkerOption[]) => void

  // Status messages
  onMessage: (message: string) => void
  onMessageClear: () => void

  // Error reporting
  onError: (error: string) => void
}
```

**`TooltipData` Structure:**
```typescript
interface TooltipData {
  columns: string[]                  // Worker names
  rows: TooltipRow[]                 // Metrics with per-worker values
  sources?: string                   // SQL source code
  attributes: Map<string, string>    // Additional node attributes
}

interface TooltipRow {
  metric: string                     // Metric name
  isCurrentMetric: boolean           // True if this drives node coloring
  cells: TooltipCell[]               // Per-worker values
}

interface TooltipCell {
  value: string                      // Formatted value
  percentile: number                 // 0-100 for heatmap coloring
}
```

**Public Methods:**
```typescript
profiler.selectMetric(metricId: string)     // Change selected metric
profiler.toggleWorker(workerId: string)     // Toggle worker visibility
profiler.toggleAllWorkers()                 // Toggle all workers
profiler.search(query: string)              // Search for node by ID
profiler.dispose()                          // Clean up resources
```

## Data Format Requirements

### Input: Profile JSON (`JsonProfiles`)

Generated by Feldera pipeline manager:

```typescript
interface JsonProfiles {
  worker_profiles: Array<{
    metadata: Map<NodeId, {
      entries: Array<Measurement>  // Performance metrics
    }>
  }>
  graph: {
    nodes: {
      id: NodeId
      label: string
      nodes: Array<SimpleNode | ClusterNode>
    }
    edges: Array<{
      from_node: NodeId
      to_node: NodeId
      from_cluster: boolean
      to_cluster: boolean
    }>
  }
}
```

### Input: Dataflow JSON (`Dataflow`)

Generated by SQL compiler with `--dataflow` flag:

```typescript
interface Dataflow {
  sources: Array<string>  // SQL source code lines
  mir: {
    [nodeId: string]: {
      operation: string         // Operator type (join, filter, etc.)
      table?: string           // Associated table
      view?: string            // Associated view
      positions: Array<{       // Source code positions
        start_line_number: number
        start_column: number
        end_line_number: number
        end_column: number
      }>
      persistent_id?: string   // Links to profile nodes
    }
  }
}
```

## Development Workflow

### Building the Library

```bash
cd packages/profiler-lib
bun install
bun run build    # Compiles TypeScript to dist/
```

Output structure:
```
dist/
├── index.js          # Main entry point
├── index.d.ts        # TypeScript declarations
├── profiler.js
├── profiler.d.ts
├── profile.js
├── profile.d.ts
├── cytograph.js
├── cytograph.d.ts
└── ...               # All modules with .js, .d.ts, .js.map, .d.ts.map
```

### Watch Mode (Development)

```bash
bun run watch    # Recompiles on file changes
```

### Clean Build

```bash
bun run clean    # Removes dist/
bun run build    # Fresh build
```

### Using in Other Packages

The library uses workspace dependencies:

```json
{
  "dependencies": {
    "profiler-lib": "workspace:*"
  }
}
```

Bun resolves this to the local package during development.

## Performance Characteristics

- **Handles 100-500 nodes**: Smooth interaction with full features
- **500-1000 nodes**: Some layout delay, but usable
- **1000+ nodes**: Consider pagination or filtering at application level
- **ELK layout**: High-quality hierarchical layout but slower than force-directed (acceptable trade-off)
- **Incremental updates**: ZSet-based diffing minimizes DOM mutations when graph changes
- **Tooltip rendering**: Limited to 40 cells per metric to prevent performance degradation with many workers

## Testing Considerations

### Unit Testing

Test individual modules with mocked data:

```typescript
import { CircuitProfile } from 'profiler-lib'

describe('CircuitProfile', () => {
  it('parses valid profile JSON', () => {
    const profile = CircuitProfile.fromJson(mockProfileData)
    expect(profile.simpleNodes.size).toBeGreaterThan(0)
  })

  it('integrates dataflow data', () => {
    const profile = CircuitProfile.fromJson(mockProfileData)
    profile.setDataflow(mockDataflowData)
    expect(profile.sources.isSome()).toBe(true)
  })
})
```

### Integration Testing

Test with real DOM and Cytoscape:

```typescript
import { Profiler, type ProfilerCallbacks } from 'profiler-lib'

describe('Profiler rendering', () => {
  let graphContainer: HTMLDivElement
  let navigatorContainer: HTMLDivElement

  beforeEach(() => {
    graphContainer = document.createElement('div')
    navigatorContainer = document.createElement('div')
    document.body.appendChild(graphContainer)
    document.body.appendChild(navigatorContainer)
  })

  afterEach(() => {
    document.body.removeChild(graphContainer)
    document.body.removeChild(navigatorContainer)
  })

  it('renders without errors', () => {
    const callbacks: ProfilerCallbacks = {
      onTooltipUpdate: () => {},
      onMetricsChanged: () => {},
      onWorkersChanged: () => {},
      onMessage: () => {},
      onMessageClear: () => {},
      onError: () => {}
    }

    const profiler = new Profiler({
      graphContainer,
      navigatorContainer,
      callbacks
    })

    expect(() => profiler.render(mockProfile)).not.toThrow()
    profiler.dispose()
  })
})
```

## Dependencies

### Peer Dependencies (Required)

These must be installed by the consuming application:

- **cytoscape** ^3.33.1 - Core graph visualization library
- **cytoscape-dblclick** ^0.3.1 - Double-click event handling
- **cytoscape-elk** ^2.3.0 - ELK layout algorithm integration
- **elkjs** ^0.11.0 - Eclipse Layout Kernel implementation

### Dev Dependencies

- **TypeScript** ^5.9.2 - Type-safe compilation
- **@types/cytoscape** ^3.21.9 - TypeScript definitions for Cytoscape

## Troubleshooting

### "Cannot find module 'cytoscape'"

Install peer dependencies:
```bash
bun install cytoscape cytoscape-dblclick cytoscape-elk elkjs
```

### Graph not rendering

1. Ensure container has explicit dimensions:
   ```css
   #graph { width: 100%; height: 80vh; }
   ```

2. Verify container is in DOM before calling `render()`:
   ```typescript
   if (!graphContainer || !document.body.contains(graphContainer)) {
     throw new Error('Container not mounted')
   }
   ```

3. Check browser console for Cytoscape errors

### Memory leaks

Always call `dispose()` when profiler is no longer needed:
```typescript
// In component lifecycle
onDestroy(() => profiler?.dispose())

// Or in cleanup function
useEffect(() => {
  const profiler = new Profiler(config)
  return () => profiler.dispose()  // Cleanup
}, [])
```

### TypeScript errors with imports

Ensure `moduleResolution: "node"` in `tsconfig.json`:
```json
{
  "compilerOptions": {
    "moduleResolution": "node",
    "esModuleInterop": true
  }
}
```

## Related Packages

- **profiler-app**: Standalone application using profiler-lib with file loading
- **web-console**: Production UI with three-layer architecture:
  - `TabProfileVisualizer.svelte` - Data loading and snapshot management
  - `ProfilerLayout.svelte` - UI controls and tooltip rendering
  - `ProfilerDiagram.svelte` - Glue layer between ProfilerLayout and profiler-lib