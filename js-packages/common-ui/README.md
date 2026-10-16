# common-ui

Shared, app-agnostic Svelte UI building blocks used across Feldera's frontends
(the web console and the standalone profiler app). Anything reusable that isn't
tied to a specific app's routes or data model lives here, so the two apps stay
visually and behaviorally consistent without copy-pasting components.

The package is consumed as the `common-ui` workspace dependency; import from its
public entry point:

```ts
import { SegmentedControl, TabsPanel, PersistentContent, MonacoEditor } from 'common-ui'
```

## What's inside

| Group | Components |
|---|---|
| Controls | `SegmentedControl`, `Select`, `TabsPanel`, `Popover`, `Tooltip` |
| Editor | `MonacoEditorRunes` and `monaco.ts`, a runes-based Monaco wrapper |
| Layout | `Drawer` shows a resizable side panel on wide screens (`InlineDrawer`) and an overlay on tablets and phones (`OverlayDrawer`). `InlineDropdown` toggles content under a header. `useIsMobile.svelte` has the screen-size media queries. |
| Layout helpers | `PersistentContent` and `persistentRect.svelte` for content that must survive conditional re-renders (see those files for details). `ANSIDecoratedText` renders ANSI-colored log output. |
| Buttons | `ClipboardCopyButton`. `ClickFeedback` shows a short confirmation state after a click. |
| Pipeline views | `ConnectorMetricsTables` shows the connector statistics of each table and view. Use `aggregateConnectorMetrics` from `common-ui/connectorMetrics` to make its input from a `/stats` response. `ConnectorErrors` shows the error lists of one connector. The caller passes the connector status, from the API or from a support bundle. |
| Health views | `StatusTimeline`, `EventLogList` and `HealthEventList` show a status history, its incidents and the events of one incident. `PipelineEventHistory` combines them for the monitor events of a pipeline, from the API or from a support bundle. |

Plain TypeScript modules that only the web console and the profiler use also
live here. Import them by their path, e.g. `common-ui/pipelineHealth`:

| Module | Contents |
|---|---|
| `statusTone` | The colors of each status tone |
| `healthStatus` | The tone, label and icon of each health status |
| `healthEvents` | The health statuses, their severity order and the incident bucket type |
| `pipelineHealth` | Pipeline monitor event types, classified and grouped into incidents |
| `felderaRelation` | SQL relation name normalization |
| `latencyColor` | Color scale for connector latencies |
| `connectorMetrics` | Connector statistics types, grouped by relation |

Code that is useful outside the web console and the profiler (formatting,
collections) lives in [`common-lib`](../common-lib/README.md) instead.

`src/lib` is the published surface (re-exported from `src/lib/index.ts`);
`src/routes` is only a local preview/showcase app and is not published.

## Developing

```sh
bun install
bun run dev      # preview/showcase app
bun run check    # type-check
```

## How it's built and consumed

Consumers import the built output (`dist/`), not the source. `bun install`
builds this package.

The `dev`, `build`, `check` and test scripts of web-console and profiler-app
first run `bun run deps`, which rebuilds this package. A running dev server does
not get your later changes. To use them, rebuild this package and restart the
dev server:

```sh
bun --cwd=js-packages/common-ui run build:self   # from the repo root
# then re-run `bun run dev` in web-console or profiler-app
```

`build` rebuilds `common-lib` first. `build:self` builds only this package.

This package was scaffolded with [`sv`](https://npmjs.com/package/sv); the
underlying tooling docs live at <https://svelte.dev/docs/kit/packaging>.
