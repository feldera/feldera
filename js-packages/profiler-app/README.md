# profiler-app

Standalone application for visualizing Feldera DBSP circuit profiles.

## Overview

Development tool for loading and visualizing circuit profiles using `profiler-lib`. Supports loading from:
- Support bundles (`.zip` files via UI or CLI)
- Local JSON files

In production, use the Web Console's integrated profiler.

## Quick Start

```bash
bun install
bun run dev
```

Opens at `http://localhost:5174` with default sample data.

## Loading Profiles

### Browser UI (Recommended)

Click **"Load Bundle"** button and select a support bundle (`.zip` file).

### CLI with Bundle

```bash
BUNDLE=/path/to/support-bundle.zip bun run dev
```

### Production Mode

```bash
bun run build
BUNDLE=/path/to/bundle.zip bun run start
```

## Environment Variables

- `BUNDLE` - Path to support bundle zip file
- `VERBOSE=1` - Enable verbose bundle processing output
- `BUNDLE_NAME` - Custom output name (default: `temp`)

## Sample Data

Included in `data/`:
- `rec.json` - Example profile
- `dataflow-rec.json` - Example dataflow graph

## Architecture

Thin wrapper around `profiler-lib`:
- HTML structure for profiler UI
- File loading via `fetch()` or bundle extraction
- Browser-based zip processing with `but-unzip`

## License

MIT
