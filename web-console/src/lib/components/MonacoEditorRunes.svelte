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

  import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'
  import jsonWorker from 'monaco-editor/esm/vs/language/json/json.worker?worker'
  import cssWorker from 'monaco-editor/esm/vs/language/css/css.worker?worker'
  import htmlWorker from 'monaco-editor/esm/vs/language/html/html.worker?worker'
  import tsWorker from 'monaco-editor/esm/vs/language/typescript/ts.worker?worker'

  self.MonacoEnvironment = {
    getWorker(_: any, label: string) {
      if (label === 'json') {
        return new jsonWorker()
      }
      if (label === 'css' || label === 'scss' || label === 'less') {
        return new cssWorker()
      }
      if (label === 'html' || label === 'handlebars' || label === 'razor') {
        return new htmlWorker()
      }
      if (label === 'typescript' || label === 'javascript') {
        return new tsWorker()
      }
      return new editorWorker()
    }
  }
</script>

<script lang="ts">
  import type Monaco from 'monaco-editor/esm/vs/editor/editor.api'
  import * as monacoImport from 'monaco-editor/esm/vs/editor/editor.api'
  import { onDestroy, onMount } from 'svelte'
  import loader from '@monaco-editor/loader'
  import { felderaCompilerMarkerSource } from '$lib/functions/pipelines/monaco'

  let monaco: typeof Monaco

  let container: HTMLDivElement
  let {
    editor = $bindable(),
    model,
    options,
    markers,
    onready
  }: {
    editor?: Monaco.editor.IStandaloneCodeEditor
    model: Monaco.editor.ITextModel
    options?: Omit<
      Monaco.editor.IStandaloneEditorConstructionOptions,
      'model' | 'value' | 'language'
    >
    markers?: Record<string, Monaco.editor.IMarkerData[]> | undefined
    onready: (event: Monaco.editor.IStandaloneCodeEditor) => void
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
      monaco.editor.removeAllMarkers(felderaCompilerMarkerSource)
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
    editor = monaco.editor.create(container, { ...options, model: null })

    onready(editor)
  })

  onDestroy(() => editor?.dispose())
</script>

<div class="monaco-container" bind:this={container}></div>

<style>
  div.monaco-container {
    width: 100%;
    height: 100%;
    padding: 0;
    margin: 0;
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
