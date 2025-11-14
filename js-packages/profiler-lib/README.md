# profiler-lib

Reusable TypeScript library for visualizing Feldera DBSP circuit profiles.

## Overview

`profiler-lib` provides interactive graph-based visualization of pipeline performance data.

## Installation

```bash
bun install profiler-lib
# Also install peer dependencies:
bun install cytoscape cytoscape-dblclick cytoscape-elk elkjs
```

## Usage

```typescript
import { Profiler, CircuitProfile, type ProfilerConfig, type JsonProfiles, type Dataflow } from 'profiler-lib';

// 1. Set up container elements in your HTML
const config: ProfilerConfig = {
    graphContainer: document.getElementById('graph')!,
    selectorContainer: document.getElementById('controls')!,
    navigatorContainer: document.getElementById('minimap')!,
    errorContainer: document.getElementById('errors'), // optional
};

// 2. Load your profile and dataflow data (from API, files, etc.)
const profileData: JsonProfiles = await fetchProfileData();
const dataflowData: Dataflow = await fetchDataflowData();

// 3. Parse the data
const profile = CircuitProfile.fromJson(profileData);
profile.setDataflow(dataflowData);

// 4. Create profiler and render
const profiler = new Profiler(config);
profiler.render(profile);

// 5. Clean up when done
profiler.dispose();
```

## API

### `Profiler`

Main class for rendering circuit profiles.

**Constructor**: `new Profiler(config: ProfilerConfig)`

**Methods**:
- `render(profile: CircuitProfile): void` - Render a circuit profile
- `dispose(): void` - Clean up resources
- `getTooltip(): HTMLElement` - Get the tooltip element
- `reportError(message: string): void` - Display an error

### `ProfilerConfig`

Configuration for the profiler.

```typescript
interface ProfilerConfig {
    graphContainer: HTMLElement;      // Main graph visualization
    selectorContainer: HTMLElement;    // Metric/worker selector controls
    navigatorContainer: HTMLElement;   // Minimap navigator
    errorContainer?: HTMLElement;      // Optional error display
}
```

### `CircuitProfile`

Represents a parsed circuit profile.

**Static Methods**:
- `fromJson(data: JsonProfiles): CircuitProfile` - Parse profile JSON
- `setDataflow(dataflow: Dataflow): void` - Integrate dataflow graph data

## Features

- **Interactive Graph**: Pan, zoom, and explore circuit operator graphs
- **Performance Metrics**: Color-coded nodes show performance hotspots
- **Hierarchical Navigation**: Expand/collapse nested circuit regions
- **SQL Source Mapping**: View SQL code that generated each operator
- **Worker-Level Details**: Compare metrics across multiple worker threads
- **Hover Tooltips**: Rich metadata display on node hover

## HTML Structure

Your HTML should include containers with the following structure:

```html
<div id="graph" style="width: 100%; height: 100vh;"></div>
<div id="controls">
    <!-- Selector UI will be injected here -->
</div>
<div id="minimap" style="width: 100px; height: 100px;"></div>
<div id="errors" style="display: none; color: red;"></div>
```

## Dependencies

- **cytoscape**: Graph visualization library
- **cytoscape-elk**: ELK hierarchical layout algorithm
- **cytoscape-dblclick**: Double-click event support
- **elkjs**: Eclipse Layout Kernel

## Type Safety

Full TypeScript support with strict type checking enabled.

## License

MIT
