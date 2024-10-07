<script context="module" lang="ts">
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
</script>

<script lang="ts">
  import type Monaco from 'monaco-editor/esm/vs/editor/editor.api'
  import * as monacoImport from 'monaco-editor/esm/vs/editor/editor.api'
  import { onDestroy, onMount } from 'svelte'
  import { createEventDispatcher } from 'svelte'
  import loader from '@monaco-editor/loader'

  let monaco: typeof Monaco

  const dispatch = createEventDispatcher<{
    ready: Monaco.editor.IStandaloneCodeEditor
  }>()

  let container: HTMLDivElement
  export let editor: Monaco.editor.IStandaloneCodeEditor | undefined = undefined
  export let value: string

  export let theme: string | undefined = undefined
  export let options: Monaco.editor.IStandaloneEditorConstructionOptions = {
    value,
    automaticLayout: true
  }
  export let markers: Record<string, Monaco.editor.IMarkerData[]> | undefined = undefined

  function refreshTheme() {
    if (theme) {
      if (exportedThemes[theme]) {
        const themeName = theme // the theme name can change during the async call
        exportedThemes[theme]().then((resolvedTheme) => {
          monaco?.editor.defineTheme(themeName, resolvedTheme as any)
          monaco?.editor.setTheme(themeName)
        })
      } else if (nativeThemes.includes(theme)) {
        monaco?.editor.setTheme(theme)
      }
    }
  }

  $: if (theme) refreshTheme()

  $: editor?.updateOptions(options)
  $: model = editor?.getModel()
  $: model && options.language ? monaco.editor.setModelLanguage(model, options.language) : void 0

  $: if (editor && editor.getValue() != value) {
    const position = editor.getPosition()
    editor.setValue(value)
    if (position) editor.setPosition(position)
  }
  $: model &&
    markers &&
    setTimeout(() => {
      Object.entries(markers).forEach(([owner, markers]) =>
        monaco.editor.setModelMarkers(model, owner, markers)
      )
    })

  onMount(async () => {
    monaco = await loader.init()
    editor = monaco.editor.create(container, options)

    dispatch('ready', editor)

    refreshTheme()

    editor.getModel()!.onDidChangeContent(() => {
      if (!editor) return
      const currentValue = editor.getValue()
      if (value === currentValue) {
        return
      }
      value = currentValue
    })
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

  div.monaco-container :global(.monaco-editor .monaco-inputbox .input) {
    box-shadow: none;
  }

  div.monaco-container :global(.monaco-editor .monaco-inputbox .input::placeholder) {
    line-height: normal;
    font-family: inherit;
    font-size: inherit;
    padding-top: inherit;
    padding-bottom: inherit;
  }
</style>
