# @feldera/monaco-editor-vite-plugin

Local fork of `@bithero/monaco-editor-vite-plugin@1.0.3` (AGPL-3.0-or-later).

## Why the fork

Upstream's `config` hook does:

```js
optimizeDeps.include = optimizeDeps.include.filter((i) => i === 'monaco-editor')
```

which *keeps only* `'monaco-editor'` and drops every other entry. The comment and log message both say "removed 'monaco-editor' from the optimizeDeps.include setting" — the intended operator is `!==`. The bug silently wipes every other `optimizeDeps.include` entry the host config added, breaking CJS interop for any pre-bundled dep (e.g. `true-json-bigint`) and causing `SyntaxError: does not provide an export named 'default'` at runtime in vitest browser tests.

The fork is a one-character fix (`===` → `!==`) plus a TS-source rewrite.

## Build

`bun run build` (runs `tsc`). Consumed from `web-console` as `workspace:*`.

## Upstream

https://codearq.net/bithero-js/monaco-editor-vite-plugin — if the upstream ever cuts a 1.0.4 with the fix, drop this fork and switch back.
