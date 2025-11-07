# profiler-app

Standalone application for visualizing Feldera DBSP circuit profiles from local files.

## Overview

This is a development tool that loads profile and dataflow JSON files from disk and renders them using `profiler-lib`. It's useful for:
- Testing profile visualizations locally
- Debugging circuit performance offline
- Developing new profiler features

In production, use the Web Console's integrated profiler instead.

## Quick Start

```bash
bun install
bun run dev
```

Open your browser to the displayed URL (typically `http://localhost:5173`).

## Loading Custom Data

### 1. Generate Profile Data

Run a Feldera pipeline with profiling enabled:

```bash
# Via REST API
curl -X POST http://localhost:8080/v0/pipelines/{pipeline_name}/circuit_profile > data/my-profile.json
```

### 2. Generate Dataflow Graph

Compile your SQL program with the `--dataflow` flag:

```bash
cd sql-to-dbsp-compiler
./SQL-compiler/sql-to-dbsp \
  --input /path/to/your-program.sql \
  --dataflow ../packages/profiler-app/data/dataflow-my-profile.json
```

### 3. Update Entry Point

Edit `src/index.ts` to load your files:

```typescript
loader.loadFiles("data", "my-profile");
```

The profiler will load:
- `data/my-profile.json` (profile)
- `data/dataflow-my-profile.json` (dataflow graph)

## Sample Data

The `data/` directory includes sample files:
- `rec.json` - Example profile data
- `dataflow-rec.json` - Example dataflow graph

## Building

```bash
bun run build    # Outputs to dist/
```

## Architecture

This app is a thin wrapper around `profiler-lib` that:
1. Provides HTML structure for the profiler UI
2. Implements file loading logic via `fetch()`
3. Wires up the profiler with DOM containers

For embedding the profiler in other applications, use `profiler-lib` directly.

## License

MIT
