# Monaco-Editor Vite Plugin — Behavior Specification

This document specifies the observable behavior of a Vite plugin whose job is
to bundle the [`monaco-editor`](https://www.npmjs.com/package/monaco-editor)
ESM build into a Vite-built application, wire up its web workers, and let the
consumer select which languages and features are compiled in.

The spec is written from the outside: what the plugin exports, what options
it accepts, how it changes the Vite configuration, and what the final bundle
must contain for monaco to work in the browser. Implementation choices
(control flow, helper function shape, variable names) are intentionally left
open.

---

## 1. Purpose

`monaco-editor` ships as a large ESM package with:

- a main entry (`monaco-editor/esm/vs/editor/editor.main.js`) that eagerly
  pulls in *all* languages and *all* editor contributions, and
- a set of web workers (`editor.worker`, `json.worker`, `css.worker`,
  `html.worker`, `ts.worker`) that must be loaded as separate bundles and
  registered through a global `MonacoEnvironment` object before the editor
  is instantiated.

Using monaco in a Vite app therefore has two friction points that this plugin
removes:

1. **Tree-shaking languages and features.** Consumers almost never want
   every language; they want (say) `json`, `sql`, `typescript`. The plugin
   rewrites the `editor.main` module to import only the selected subset.
2. **Worker wiring.** Vite's `?worker` import suffix is the idiomatic way
   to ship a worker as a separate chunk. The plugin generates the
   `MonacoEnvironment.getWorker` dispatcher that maps monaco's internal
   worker labels to those `?worker`-imported constructors.

A consumer's `vite.config.ts` should look like:

```ts
import { defineConfig } from 'vite'
import { monaco } from '<this-plugin>'

export default defineConfig({
  plugins: [
    monaco({
      languages: ['json', 'sql', 'typescript'],
      features: ['!gotoSymbol'],           // all features except one
      globalAPI: false,
    }),
  ],
})
```

After bundling, the consumer's app can `import * as monaco from 'monaco-editor'`
and call `monaco.editor.create(...)` without doing any worker setup itself.

---

## 2. Public API

### 2.1 Default export

The plugin exposes a single factory function:

```ts
export function monaco(options?: MonacoOptions): Plugin
```

where `Plugin` is the standard `vite` plugin type. It returns a single
plugin object; it does not return an array.

### 2.2 Options

```ts
interface MonacoOptions {
  features?: Features
  languages?: Languages
  customLanguages?: IFeatureDefinition[]
  globalAPI?: boolean
}

type Languages =
  | '*'
  | 'all'
  | EditorLanguage[]                       // names from monaco's metadata

type Features =
  | '*'
  | 'all'
  | Array<'codicons' | EditorFeature | NegatedEditorFeature>
//            NegatedEditorFeature is `!${EditorFeature}`
```

`EditorLanguage`, `EditorFeature`, `NegatedEditorFeature`, and
`IFeatureDefinition` are re-exported from
`monaco-editor/esm/metadata.js` — the plugin MUST NOT duplicate that list;
it MUST read it at plugin-construction time.

Semantics of each option:

| Option            | Default                    | Meaning                                                                                     |
|-------------------|----------------------------|---------------------------------------------------------------------------------------------|
| `languages`       | `[]` (nothing)             | `'*'` / `'all'` → every built-in language. Array → resolve each name against monaco's metadata list; unknown names produce a `console.error` and are dropped. Empty array → only `customLanguages`. |
| `customLanguages` | `[]`                       | Additional `IFeatureDefinition` entries appended after the resolved built-ins.              |
| `features`        | `undefined` → all features | Same `'*'`/`'all'` shorthands. An array of plain names is an allow-list. An array whose entries are all `!`-prefixed is a deny-list ("everything except these"). Mixing `!`-prefixed and plain names in the same array is not a supported combination; the implementation may pick a deterministic behavior for that case but need not document it. |
| `globalAPI`       | `false`                    | If `true`, emitted `MonacoEnvironment` sets `globalAPI: true`, which tells monaco to publish the `monaco` object as a global in the browser. |

### 2.3 Plugin object

The returned plugin has:

```ts
{
  name: <any stable plugin name — used by Vite diagnostics>,
  enforce: 'pre',           // must run before Vite's own dep-scanner
  config(config) { ... },
  load(id) { ... },
}
```

No other Vite hooks are required.

---

## 3. Behavior

### 3.1 Eager option resolution

On plugin instantiation (i.e. inside `monaco(options)` *before* returning
the plugin object), the plugin MUST resolve:

- the final list of `IFeatureDefinition` for **languages** (built-ins matching
  the `languages` option, plus `customLanguages`),
- the final list of `IFeatureDefinition` for **features**, and
- the list of **workers** that those definitions reference.

These resolved lists feed the `load` hook. They do not change per-build.

#### Worker resolution

The worker list is built by walking the union of:

- a **mandatory** `editorWorkerService` entry describing monaco's base worker:
  - label `editorWorkerService`
  - id `vs/editor/editor`
  - entry `vs/editor/editor.worker`
- every resolved language definition that has a `.worker` property,
- every resolved feature definition that has a `.worker` property.

For each, collect `{ label, id, entry }`. Duplicates should be avoided but
are not strictly harmful.

#### Codicons special case

Monaco versions older than 0.55 stored the `codicons` stylesheet at
`vs/base/browser/ui/codicons/codiconStyles.js` rather than as a
`metadata.features` entry. If that file exists on disk (resolved via the
monaco-path resolver below), inject a synthetic feature definition:

```ts
{ label: 'codicons', entry: 'vs/base/browser/ui/codicons/codiconStyles.js' }
```

so that consumers can request `'codicons'` in `features`. Also accept
`'codicon'` (singular) as an alias for `'codicons'` and vice versa — if
one is present in the registry and the user asks for the other, silently
map between them.

### 3.2 Resolving a monaco-relative path

The plugin must locate files *inside* the installed `monaco-editor/esm/…`
tree from its own runtime (Node, inside Vite), so that an entry such as
`vs/language/json/json.worker` can be turned into an absolute path that
is valid as a module specifier in the emitted replacement source.

Requirements:

- Given a relative path such as `vs/editor/editor.worker`, return an
  absolute filesystem path to the matching file inside the consumer's
  installed `monaco-editor/esm/` directory.
- Prefer node module resolution (e.g. `import.meta.resolve`,
  `createRequire().resolve`, or equivalent) so that monorepo hoisting and
  `pnpm`/`bun`/`yarn` layouts are honoured; fall back to
  `<cwd>/node_modules/monaco-editor/esm/<file>` if the direct resolution
  fails.
- If the resolver returns a `file://` URL, strip the scheme before
  returning the path.

The returned path is used verbatim in the emitted `import '<path>'` and
`import X from '<path>?worker'` statements, so it must be a string Vite
will accept as a module specifier at build time.

### 3.3 `config` hook

Purpose: prevent Vite's dependency optimizer from pre-bundling
`monaco-editor`. Vite's optimizer would flatten all of monaco's internals
into a single CJS-esque bundle, defeating the selective imports that the
`load` hook emits.

Steps:

1. Ensure `config.optimizeDeps` exists (create `{}` if absent).
2. Ensure `config.optimizeDeps.exclude` exists (create `[]` if absent).
3. Push `'monaco-editor'` onto `exclude`.
4. If `config.optimizeDeps.include` is set:
   - Optionally log a one-line notice that the plugin is removing
     `'monaco-editor'` from the include list.
   - Replace `include` with the same list **with every `'monaco-editor'`
     entry removed**, preserving all other entries. The observable
     requirement: after the `config` hook runs, `optimizeDeps.include`
     must contain no `'monaco-editor'` entry and must still contain
     every unrelated entry the consumer had added.

### 3.4 `load` hook

The `load` hook intercepts three specific module IDs. Match the id with
a regular expression that tolerates both POSIX and Windows path separators:

| Matched id                                         | Return value                                                                 |
|----------------------------------------------------|------------------------------------------------------------------------------|
| `…/esm/vs/editor/editor.main.js`                   | **Generated module source** (see 3.4.1).                                     |
| `…/esm/vs/editor/editor.all.js`                    | A module that throws at import time with a message directing the user to `editor.main.js` or plain `monaco-editor`. |
| `…/esm/vs/editor/edcore.main.js`                   | Same as `editor.all.js` — a throwing stub.                                   |
| anything else                                      | Return `undefined` so Vite's normal resolution continues.                    |

Regex hint:

```js
/esm[/\\]vs[/\\]editor[/\\]editor\.main\.js/
```

#### 3.4.1 Generated `editor.main.js` replacement

The emitted source must, in order:

1. **Import every worker** resolved in 3.1, using Vite's `?worker` suffix
   so each becomes its own chunk. Illustrative shape:

   ```js
   import <someBinding> from '<abs path to worker entry>?worker'
   ```

   — one such import per resolved worker. The `<abs path>` comes from the
   monaco-path resolver (3.2) applied to the worker's `entry` field.
   The local binding name is implementation-defined; it only has to be
   unique within the emitted module and referenced consistently in the
   `MonacoEnvironment` object emitted in step 2.

2. **Install `MonacoEnvironment`** on `self`. The emitted code must assign
   `self.MonacoEnvironment` an object with two fields:

   - `globalAPI` — the boolean value of the `globalAPI` option.
   - `getWorker(moduleId, label)` — a function that, given monaco's
     `label` argument, returns a `new Worker(...)` instance built from
     the corresponding `?worker`-imported constructor. Labels map to
     the `label` field of each worker resolved in step 1
     (e.g. `editorWorkerService`, `json`, `css`, `html`, `typescript`).

   The exact expression used to build the lookup (inline `switch`,
   object literal of thunks, `Map`, closure-captured table, etc.) is
   up to the implementation. What matters is the observable behavior:
   calling `getWorker(_, 'json')` returns a `Worker` built from the
   `json.worker` module, and an unknown label either falls through or
   throws — pick one and be consistent.

3. **Eagerly import every resolved feature**, for side effects only:

   ```js
   import "<abs path to feature.entry[0]>"
   import "<abs path to feature.entry[1]>"
   // ...
   ```

   A feature's `entry` field can be a string or an array of strings; flatten
   before emitting.

4. **Eagerly import every resolved language**, same pattern:

   ```js
   import "<abs path to language.entry>"
   ```

   Languages also may have array-valued `entry` fields — flatten.

5. **Re-export monaco's public API** so that downstream
   `import * as monaco from 'monaco-editor'` keeps working:

   ```js
   export * from './editor.api.js';
   ```

   The relative `./editor.api.js` is correct because the emitted module
   *replaces* `editor.main.js`, which lives in the same directory as
   `editor.api.js` inside monaco's ESM tree.

The generator should produce a single string (newline-joined) and return it
from `load`. Vite will then pipe that string through the rest of its plugin
chain as if it were the real `editor.main.js`.

#### 3.4.2 `editor.all.js` / `edcore.main.js` stubs

Both of these monaco entry points pull in *everything* and bypass the
selection this plugin is trying to enforce. To keep consumers from
accidentally importing them, return a module whose evaluation throws
at import time with a message directing the reader at the correct entry
point (`esm/vs/editor/editor.main.js`, or the plain `monaco-editor`
package export). The exact message wording is the implementer's choice;
a top-level `throw` statement is sufficient and the thrown value need not
be an `Error` instance.

---

### 3.5 References for the worker-wiring pattern

The `MonacoEnvironment.getWorker` + per-worker `?worker` imports pattern
described in 3.4.1 is **not specific to this plugin**. It is the standard
approach published by the monaco-editor project itself and by Vite:

- monaco-editor README, "Integrate the ESM version of the Monaco Editor"
  section — documents the `self.MonacoEnvironment = { getWorker(moduleId, label) { … } }`
  contract, including the set of labels (`json`, `css`, `html`,
  `typescript`/`javascript`, and the default `editorWorkerService`).
- monaco-editor samples repo (`monaco-editor-samples`) — ships working
  end-to-end examples that wire each worker with `new Worker(new URL(...))`
  inside `getWorker`.
- Vite docs, "Web Workers" — documents the `?worker` import suffix as the
  supported way to bundle a Web Worker as its own chunk, yielding a
  default-exported constructor.

An implementer building this plugin from the present spec should consult
those upstream sources for the exact worker labels, the `MonacoEnvironment`
shape, and `?worker`-import semantics.

---

## 4. Peer dependencies

- `monaco-editor >= 0.44.0` (peer).
- `vite >= 5` (peer / dev).

The plugin imports `monaco-editor/esm/metadata.js` at plugin construction
time to read the language and feature catalog. It does **not** bundle a
copy of that metadata.

---

## 5. Expected end-user result

After this plugin runs, a production build of a Vite app that does
`import * as monaco from 'monaco-editor'` should:

- Emit one chunk per selected worker (`editor.worker-<hash>.js`,
  `json.worker-<hash>.js`, …) thanks to Vite's `?worker` handling.
- Emit one "editor" chunk containing `editor.api.js` plus only the
  selected languages and features — not the full monaco catalog.
- Install `self.MonacoEnvironment` on first import so `monaco.editor.create`
  works without the consumer wiring workers manually.
- Not pre-bundle `monaco-editor` through Vite's dependency optimizer.

---

## 6. Non-goals

- Supporting non-Vite bundlers (webpack, rollup, esbuild standalone).
- Supporting monaco's AMD loader build — ESM only.
- Providing a runtime API; the plugin is build-time only.
- Hot-swapping the language/feature selection after the plugin has been
  constructed — options are read once.

---

## 7. Test checklist

Suggested coverage for a fresh implementation:

1. `monaco({ languages: ['json'] })` produces a bundle whose `editor.main`
   chunk references `vs/language/json/…` entries and **does not** reference
   `vs/language/typescript/…`.
2. `monaco({ languages: '*' })` produces a bundle that references every
   language entry listed in `monaco-editor/esm/metadata.js`.
3. `monaco({ features: ['!gotoSymbol'] })` excludes the `gotoSymbol`
   feature and keeps the rest.
4. `monaco({ languages: ['nonesuch'] })` emits a `console.error` with the
   unknown-language name and drops it from the output.
5. `config` hook:
   - Creates `optimizeDeps` and `optimizeDeps.exclude` when absent.
   - Appends `'monaco-editor'` to `exclude`.
   - Given `include: ['lodash', 'monaco-editor', 'date-fns']`, leaves
     `['lodash', 'date-fns']`.
6. `load` hook returns `undefined` for unrelated ids (passthrough).
7. `load` hook on `editor.all.js` returns a module whose evaluation throws.
8. Worker wiring: the generated `MonacoEnvironment.getWorker('_', 'json')`
   returns a `Worker` instance when exercised in a browser integration test.
9. `globalAPI: true` causes `window.monaco` to be defined after first
   editor import in a browser integration test; `globalAPI: false` does not.
10. Codicons back-compat: with a monaco version that has the legacy
    `codiconStyles.js` file, `features: ['codicons']` resolves and emits
    the corresponding import; without that file, it resolves to whatever
    `metadata.features` contains (under either `codicon` or `codicons`).
