# profiler-app

Standalone application for visualizing Feldera DBSP circuit profiles.

## Overview

This is a SPA web app for loading and visualizing circuit profiles using `profiler-lib`. Supports loading profiles from support bundle `.zip` files.

Provides the same functionality as the Web Console's integrated profiler.

## Quick Start

```bash
bun install
bun run dev
```

Web app is available at `http://localhost:5174`.

To apply the changes in the `profiler-lib` project to `profiler-app`, run `cd js-packages/profiler-lib && bun run build`. If you have a dev build running, the app will update on the fly.

## Loading Profiles

Click the **"Load Bundle"** button and select a support bundle `.zip` file to load.

## Build Production App

```bash
bun run build
```

The app is compiled into a single dist/index.html. It can be served by a server or opened directly in the browser.

## Sample Data

Included in `data/`:
- `rec.json` - Example profile

## Architecture

Thin wrapper around `profiler-lib`:
- HTML structure for profiler UI
- Browser-based .zip archive processing with `but-unzip`

## License

MIT
