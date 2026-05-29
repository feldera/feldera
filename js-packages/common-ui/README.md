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

- **Controls** — `SegmentedControl`, `Select`, `TabsPanel`, `Popover`, `Tooltip`.
- **Editor** — `MonacoEditorRunes` / `monaco.ts`: a runes-based Monaco wrapper.
- **Layout helpers** — `PersistentContent` + `persistentRect.svelte` for content
  that must survive conditional re-renders (see those files for details),
  `ANSIDecoratedText` for rendering ANSI-colored log output.

`src/lib` is the published surface (re-exported from `src/lib/index.ts`);
`src/routes` is only a local preview/showcase app and is not published.

## Developing

```sh
bun install
bun run dev      # preview/showcase app
bun run check    # type-check
```

## How it's built and consumed

Consumers import the **built** output (`dist/`), not the source. Building `web-console` or `profiler-app`
(`bun run build`) rebuilds this package automatically — their `prebuild` step
runs `build:deps:*` at the repo root, which includes
`bun --cwd=js-packages/common-ui run build`.

`bun run dev`, however, does **not** run that step, so it uses whatever is
already in `dist/`. After changing anything in `common-ui` while developing an
app, rebuild this package and restart the dev server:

```sh
bun --cwd=js-packages/common-ui run build   # from the repo root
# then re-run `bun run dev` in web-console or profiler-app
```

This package was scaffolded with [`sv`](https://npmjs.com/package/sv); the
underlying tooling docs live at <https://svelte.dev/docs/kit/packaging>.
