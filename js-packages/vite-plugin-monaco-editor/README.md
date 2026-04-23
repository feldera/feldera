# @feldera/vite-plugin-monaco-editor

A Vite plugin that bundles the ESM build of [`monaco-editor`](https://www.npmjs.com/package/monaco-editor) with only the languages, features, and workers the consuming app actually uses, and wires `self.MonacoEnvironment` so the editor Just Works in the browser.

See `../monaco-vite-plugin.spec.md` for the normative behavior specification; this README is the field notes — what monaco does that makes naive bundling blow up, what the plugin does about it, and how a consumer has to import monaco for tree-shaking to actually land.

## Usage

```ts
// vite.config.ts
import { monaco } from '@feldera/vite-plugin-monaco-editor'

export default defineConfig({
  plugins: [
    monaco({
      languages: ['json', 'sql', 'rust', 'graphql'],
      features: [
        'browser', 'clipboard', 'comment', 'find', 'folding', 'format',
        'gotoLine', 'gotoSymbol', 'hover', 'inPlaceReplace', 'inspectTokens',
        'iPadShowKeyboard', 'linesOperations', 'links', 'multicursor',
        'parameterHints', 'quickOutline', 'smartSelect', 'suggest',
        'wordHighlighter', 'wordOperations'
      ]
    })
  ]
})
```

The `features` / `languages` / `customLanguages` / `globalAPI` options and their semantics are defined in the spec.

## How monaco breaks naive tree-shaking

The monaco ESM tree ships with three layers of "pull everything in" that a bundler cannot undo on its own:

1. **`esm/vs/editor/editor.main.js`** — the package's main entry. Imports every built-in language contribution and every editor contribution as bare side-effect imports. This file is what `import 'monaco-editor'` resolves to.

2. **Per-language contribution files** — e.g. `esm/vs/language/json/monaco.contribution.js`, `esm/vs/language/css/monaco.contribution.js`. Each of these *also* re-imports every editor contrib as bare side-effect imports before doing any language setup. Touching any language bundles every contrib.

3. **Shared infrastructure with the same pattern** — the same bare-contrib-import block is copy-pasted into:
   - `esm/vs/basic-languages/_.contribution.js` (loader base for the `basic-languages` tree)
   - `esm/vs/language/common/lspLanguageFeatures.js` (shared LSP plumbing used by json/css/html/ts modes)
   - `esm/vs/common/workers.js` (imported by every language's `workerManager.js`)

   So even after you strip `editor.main.js`, as soon as one language mode loads, all three of these files drag the full contrib catalog back in.

Consequence: replacing only `editor.main.js` (what most monaco-vite plugins do) is *not* enough. You also have to neutralise the side-effect import blocks in (2) and (3) or the contrib graph survives.

## What this plugin does

### `editor.main.js` replacement

The `load` hook intercepts `monaco-editor/esm/vs/editor/editor.main.js` and emits a replacement module that contains only:

- One `?worker`-suffixed import per worker that the resolved language / feature set actually references (`editor.worker` is always included; `json.worker`, `css.worker`, `html.worker`, `ts.worker` only if the respective language is selected).
- A `self.MonacoEnvironment = { globalAPI, getWorker(_, label) { ... } }` dispatch that maps monaco's internal worker labels (including `javascript` → ts-worker, `handlebars`/`razor` → html-worker) to those `?worker`-imported constructors.
- One bare `import '<abs path>'` per selected *feature* entry (from `metadata.features`).
- One bare `import '<abs path>'` per selected *language* entry (from `metadata.languages` + any `customLanguages`).
- `export * from './editor.api.js'` so `import * as monaco from 'monaco-editor'` keeps returning monaco's public API.

This turns `import * as monaco from 'monaco-editor'` into a statically-analysable subset.

### Contrib-import stripping for the rest of `monaco-editor/esm/**/*.js`

Because of the three-layer problem above, the `load` hook *also* intercepts every `monaco-editor/esm/vs/**/*.js` file, reads it, and strips bare side-effect imports whose specifier matches `…/editor/(contrib|standalone/browser)/…`. Imports with bindings (`import { X } from …`) are untouched — only `import '…';` statements are removed, and only when the specifier points into the contrib or standalone-add-on trees.

The rule is deliberately shape-based rather than a hand-maintained allow-list of filenames, because the set of files that adopt this copy-pasted import block grows per monaco release. If a future monaco version adds a fourth file with the same pattern, the plugin handles it without a version bump.

The plugin returns `undefined` (no transform) for files that contain no such imports, so Rolldown/Rollup keeps its own cache keys stable.

### `editor.all.js` / `edcore.main.js`

These monaco entry points bypass selection and re-export everything. The `load` hook replaces them with a module that throws at import time, pointing the reader at `editor.main.js` or the plain `monaco-editor` package export.

### `config` hook

`monaco-editor` is pushed onto `optimizeDeps.exclude` and removed from `optimizeDeps.include` (if the consumer added it), so Vite's dep pre-bundler doesn't flatten the ESM tree before our `load` hook can transform it.

## What the *consumer* has to do

The plugin only transforms files it sees in `load`. For that to happen, consumer code has to route its monaco imports through paths that resolve inside `monaco-editor/esm/…` — that is, either through the package entry (`'monaco-editor'`) or through an explicit `monaco-editor/esm/…` subpath.

### Import the package entry, not `editor.api.js`

```ts
// Good — goes through our editor.main.js replacement
import * as monaco from 'monaco-editor'
import { editor, KeyCode, KeyMod, MarkerSeverity } from 'monaco-editor'
import type { editor } from 'monaco-editor'
```

```ts
// Bad — bypasses the plugin. Still works, but pulls in the full editor
// graph via editor.api.js → editor.api2.js → standaloneEditor.js → ...
import { editor } from 'monaco-editor/esm/vs/editor/editor.api.js'
```

The deep import is what most monaco snippets on the web use. Migrating it to the package entry is what actually produces the bundle-size delta — stripping `editor.main.js` in the plugin doesn't help if no file in the app imports `'monaco-editor'`.

### Workers are wired for you

Because the generated `editor.main.js` replacement sets `self.MonacoEnvironment`, consumers who `import 'monaco-editor'` don't need their own `MonacoEnvironment` block or `?worker` imports. An app that *also* registers its own `MonacoEnvironment` will win (last write), which is fine for incremental migrations.

## Verifying that tree-shaking actually happened

After a production build:

- `_app/immutable/workers/` should contain only the workers for languages you selected — no `ts.worker-*.js` if you didn't ask for `typescript`, etc.
- `grep` the biggest chunk for classes that belong to *unselected* contribs — e.g. `BracketMatchingController`, `CodeLensContribution`, `ColorDetector`, `InlineCompletionsController`, `RenameAction`, `StickyScrollController`, `UnicodeHighlighter`, `EditorFontZoomIn`. If any of these appear for contribs you didn't list in `features`, something is pulling monaco in through a path that skipped the plugin.

Caveats when reading such a grep:

- `StickyScrollController` also exists as a class in `vs/base/browser/ui/tree/` (the tree-widget component), unrelated to the `stickyScroll` editor contrib. A handful of hits from the tree widget are expected even when the contrib is excluded.
- Some selected features have genuine transitive contrib dependencies (`hover` uses pieces of `colorPicker`; `suggest` uses `inlineCompletions` utilities). Those aren't a bug — they're what you asked for.

## Non-goals

Unchanged from the spec: webpack/rollup/esbuild-standalone support, monaco's AMD loader, runtime API, and hot-swapping the language/feature set after plugin construction are all out of scope.
