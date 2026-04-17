# js-packages

Monorepo of TypeScript/JavaScript packages for Feldera's frontend and tooling. All packages use **Bun** as the package manager.

## Packages

### web-console
Production SvelteKit 2.x dashboard for pipeline management, SQL editing, real-time monitoring, and profiling. Deployed as a static site served by pipeline-manager. See `web-console/CLAUDE.md`.

### profiler-app
Standalone single-page app for offline profile visualization. Builds to a single self-contained HTML file. See `profiler-app/CLAUDE.md`.

### profiler-layout
Reusable Svelte 5 component library for profiler UI (`ProfilerLayout`, `ProfilerDiagram`, `ProfilerTooltip`, `ProfileTimestampSelector`) and support bundle processing utilities (`getSuitableProfiles()`, `processProfileFiles()`). See `profiler-layout/CLAUDE.md`.

### profiler-lib
Core visualization engine using Cytoscape.js and ELK layout. Callback-based API for UI integration with incremental circuit graph rendering and performance metric display. See `profiler-lib/CLAUDE.md`.

### feldera-theme
Pure CSS theme package (Skeleton UI customization with Feldera branding — purple gradients, DM Sans fonts).

### monaco-editor-vite-plugin
Local fork of `@bithero/monaco-editor-vite-plugin@1.0.3` (AGPL-3.0-or-later) with a single bug fix: upstream's `config` hook inverted the filter operator and silently wiped all other entries from `optimizeDeps.include`. Consumed by web-console only. See `monaco-editor-vite-plugin/CLAUDE.md`.

### triage-types
Shared TypeScript types for the support bundle triage plugin system. Defines `TriagePlugin`, `TriageRuleResult`, `TriageResults`, `Severity`, and `DecodedBundle` interfaces used by triage plugins in the cloud repo.

## Dependency Graph

```
web-console -> profiler-layout, feldera-theme, triage-types, monaco-editor-vite-plugin
profiler-app -> profiler-layout, feldera-theme
profiler-layout -> profiler-lib
```

Build order matters: profiler-lib → profiler-layout → web-console/profiler-app.

## Development Commands

From repository root:
- `bun install` — install all workspace dependencies
- `bun run -r build` — build all packages
- `bun run -r check` — type check all packages

Per package: `bun run dev`, `bun run build`, `bun run check`.

## Shared Technologies

- **Svelte 5** with runes (`$state`, `$derived`, `$effect`, `$bindable`)
- **SvelteKit 2.x** (web-console), **Vite** (all packages)
- **TailwindCSS** + **Skeleton UI** + **feldera-theme**
- **TypeScript** with strict checking throughout

## Integration Points

- **web-console ↔ pipeline-manager**: OpenAPI-generated client, WebSocket for real-time logs/metrics
- **profiler-app**: Fully offline, browser file picker for bundle uploads
- **profiler-layout**: Shared between web-console and profiler-app via Svelte component composition
- **triage-types**: Consumed by support-bundle-triage package in the cloud repo
