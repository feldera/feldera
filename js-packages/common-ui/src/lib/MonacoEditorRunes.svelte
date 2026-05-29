<script lang="ts" module>
  loader.config({ monaco: monacoImport, 'vs/nls': { availableLanguages: { '*': 'en' } } })

  export const exportedThemes = Object.fromEntries(
    Object.entries(import.meta.glob('/node_modules/monaco-themes/themes/*.json')).map(([k, v]) => [
      k.toLowerCase().split('/').reverse()[0].slice(0, -'.json'.length).replaceAll(' ', '-'),
      v
    ])
  )

  export const nativeThemes = ['vs', 'vs-dark', 'hc-black']

  export const themeNames: string[] = [...Object.keys(exportedThemes), ...nativeThemes].sort(
    (a, b) => a.localeCompare(b)
  )

  // Worker setup is owned by `@feldera/vite-plugin-monaco-editor`: the plugin
  // generates a `MonacoEnvironment.getWorker` whose worker imports honor its
  // selected languages and `inlineWorkers` option. Consumers MUST configure
  // that plugin in their Vite config; this component does not bundle workers.
</script>

<script lang="ts">
  import type Monaco from 'monaco-editor'
  import * as monacoImport from 'monaco-editor'
  import { onDestroy, onMount } from 'svelte'
  import loader from '@monaco-editor/loader'

  let monaco: typeof Monaco

  let container: HTMLDivElement
  let {
    editor = $bindable(),
    model,
    options,
    markers,
    markerSource,
    jsonSchemas,
    onready,
    extras
  }: {
    editor?: Monaco.editor.IStandaloneCodeEditor
    model: Monaco.editor.ITextModel
    options?: Omit<
      Monaco.editor.IStandaloneEditorConstructionOptions,
      'model' | 'value' | 'language'
    >
    markers?: Record<string, Monaco.editor.IMarkerData[]> | undefined
    /** When `markers` is cleared, markers owned by this source are removed. */
    markerSource?: string
    /** Optional JSON schemas to register for `json`-language validation. */
    jsonSchemas?: Monaco.json.DiagnosticsOptions['schemas']
    onready: (event: Monaco.editor.IStandaloneCodeEditor) => void
    extras?: {
      isDarkMode?: boolean
    }
  } = $props()

  $effect(() => {
    if (!editor) {
      return
    }
    if (model.uri === editor.getModel()?.uri) {
      return
    }
    editor.setModel(model)
  })

  $effect(() => {
    editor?.updateOptions(options ?? {})
  })

  $effect(() => {
    markers
    if (!monaco) {
      return
    }
    if (!model) {
      return
    }
    if (!markers) {
      if (markerSource) {
        monaco.editor.removeAllMarkers(markerSource)
      }
      return
    }
    setTimeout(() => {
      Object.entries(markers).forEach(([owner, markers]) =>
        monaco.editor.setModelMarkers(model, owner, markers)
      )
    })
  })

  onMount(async () => {
    monaco = await loader.init()
    if (jsonSchemas) {
      // `jsonDefaults` is a named export of the json `monaco.contribution`,
      // NOT a property of the runtime `monaco` object — monaco only publishes
      // the `editor` and `languages` namespaces on the aggregate. We dynamic-
      // import it here; when the consumer enabled `'json'` in
      // `@feldera/vite-plugin-monaco-editor`, this is a cache hit on the
      // module that the plugin already statically loaded, and when they
      // didn't, the plugin suppresses the import (the returned module is
      // empty) and we warn instead of silently shipping a broken validator.
      const { jsonDefaults } = (await import(
        'monaco-editor/esm/vs/language/json/monaco.contribution.js'
      )) as { jsonDefaults?: Monaco.json.LanguageServiceDefaults }
      if (jsonDefaults && !jsonDefaults.diagnosticsOptions.schemas?.length) {
        jsonDefaults.setDiagnosticsOptions({
          validate: true,
          schemas: jsonSchemas
        })
      } else if (!jsonDefaults) {
        console.warn(
          "[MonacoEditor] `jsonSchemas` was provided but the `json` language isn't enabled in @feldera/vite-plugin-monaco-editor — schemas ignored."
        )
      }
    }
    editor = monaco.editor.create(container, {
      // TODO: Workaround for Windows-only cursor mis-positioning on mouse click
      // (cursor lands progressively further off the clicked glyph along the line;
      // a line that contains an emoji renders correctly because Monaco's
      // monospace fast path is disabled per-line for complex characters).
      // Suspected cause: at editor-create time the configured webfont has not
      // finished loading, so Monaco measures `typicalHalfwidthCharacterWidth`
      // against a fallback font; on Windows this fallback's advance width
      // differs from the real font, on Linux it happens to match — hence the
      // OS asymmetry. Forcing the variable-width path globally avoids the bad
      // cached measurement at the cost of a small per-render measurement.
      // Proper fix to try once a Windows test machine is available: drop this
      // option and instead `await document.fonts.ready` before
      // `monaco.editor.create`, plus call `monaco.editor.remeasureFonts()`
      // after any subsequent webfont swap.
      disableMonospaceOptimizations: true,
      ...options,
      model: null
    })

    onready(editor)
  })

  onDestroy(() => editor?.dispose())
</script>

<div
  class="h-full w-full p-0 m-0 {options?.readOnly
    ? extras?.isDarkMode
      ? 'monaco-readonly-dark'
      : 'monaco-readonly'
    : ''}"
  bind:this={container}
></div>

<style>
  .monaco-readonly {
    :global(.sticky-line-content, .sticky-widget-line-numbers, .margin, .monaco-editor-background) {
      background-color: var(--color-surface-50);
    }
  }
  .monaco-readonly-dark {
    :global(.sticky-line-content, .sticky-widget-line-numbers, .margin, .monaco-editor-background) {
      background-color: var(--color-surface-950);
    }
  }

  div :global(.monaco-editor .monaco-inputbox .input) {
    box-shadow: none;
  }

  div :global(.monaco-editor .monaco-inputbox .input::placeholder) {
    line-height: normal;
    font-family: inherit;
    font-size: inherit;
    padding-top: inherit;
    padding-bottom: inherit;
  }
</style>
