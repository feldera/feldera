# profiler-layout

Reusable Svelte component library for visualizing Feldera DBSP circuit profiles.

## Overview

Provides shared UI components and utilities for circuit profile visualization across web-console and profiler-app. Built with Svelte 5 and packaged for distribution to consuming applications.

## Quick Start

```bash
bun install
```

## Build Package

```bash
bun run build
```

Outputs to `dist/` as an ESM library package. Automatically rebuilds `profiler-lib` dependency.

Runthe build to test changes in consuming apps.

## Exported Components

- **`ProfilerLayout`** - Main visualization container with toolbar slots
- **`ProfilerDiagram`** - Circuit graph renderer
- **`ProfilerTooltip`** - Node metrics details display
- **`ProfileTimestampSelector`** - Timestamp navigation dropdown

## Exported Utilities

- **`getSuitableProfiles(zipData)`** - Extract profile timestamps from support bundle (unzips once)
- **`processProfileFiles(files)`** - Parse profile data from unzipped files

## Architecture

Presentation layer between profiler-lib (visualization engine) and applications:
- Svelte 5 components with runes-based reactivity
- Shared bundle processing utilities
- Two-way binding support via `$bindable`
- Consistent UX patterns across applications

## Usage

```typescript
import {
  ProfilerLayout,
  ProfileTimestampSelector,
  getSuitableProfiles,
  processProfileFiles
} from 'profiler-layout'

// Extract profiles from bundle
const profiles = getSuitableProfiles(zipData)

// Process specific timestamp
const data = await processProfileFiles(profiles[0][1])

// Render in Svelte component
<ProfilerLayout profileData={data.profile} dataflowData={data.dataflow} programCode={data.sources} />
```

## License

MIT
