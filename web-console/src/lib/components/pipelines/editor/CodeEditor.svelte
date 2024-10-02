<script lang="ts">
  import type { Snippet } from 'svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { DecoupledState } from '$lib/compositions/decoupledState.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import MonacoEditor, { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import * as MonacoImports from 'monaco-editor'
  import { editor } from 'monaco-editor/esm/vs/editor/editor.api'
  import type { EditorLanguage } from 'monaco-editor/esm/metadata'
  import PipelineEditorStatusBar from '$lib/components/layout/pipelines/PipelineEditorStatusBar.svelte'
  import { page } from '$app/stores'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { pipelineFileNameRegex } from '$lib/compositions/health/systemErrors'
  void MonacoImports // Explicitly import all monaco-editor esm modules

  let {
    path,
    files,
    currentFileName = $bindable(),
    editDisabled,
    textEditor,
    statusBarCenter,
    statusBarEnd
  }: {
    path: string
    files: {
      name: string
      access: { current: string }
      language?: EditorLanguage
      markers?: Record<string, editor.IMarkerData[]>
    }[]
    currentFileName: string
    editDisabled?: boolean
    textEditor: Snippet<[children: Snippet]>
    statusBarCenter?: Snippet
    statusBarEnd?: Snippet<[downstreamChanged: boolean]>
  } = $props()

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)

  let wait = $derived(autoSavePipeline.value ? 2000 : ('decoupled' as const))
  let file = $derived(files.find((f) => f.name === currentFileName)!)

  function isReadonlyProperty<T>(obj: T, prop: keyof T) {
    return !Object.getOwnPropertyDescriptor(obj, prop)?.['set']
  }
  let isReadonly = $derived(editDisabled || isReadonlyProperty(file.access, 'current'))

  let pipelineName = $derived(path)
  let filePath = $derived(pipelineName + '/' + file.name)
  let editedFiles: Record<string, DecoupledState<string>> = (() => ({
    [filePath]: new DecoupledState(file.access, () => wait)
  }))()
  {
    // TODO: handle remote update of the program code that conflicts with currently edited version
    $effect.pre(() => {
      editedFiles[filePath] ??= new DecoupledState(file.access, () => wait)
    })
  }

  $effect(() => {
    // Trigger save right away when autosave is turned on
    if (!autoSavePipeline.value) {
      return
    }
    setTimeout(() => Object.values(editedFiles).forEach((file) => file.push()))
  })

  let editorRef: editor.IStandaloneCodeEditor = $state()!
  $effect(() => {
    if (!editorRef) {
      return
    }
    const [, fileName, line, , column] =
      $page.url.hash.match(new RegExp(`#(${pipelineFileNameRegex}):(\\d+)(:(\\d+))?`)) ?? []
    if (!line) {
      return
    }
    if (currentFileName !== fileName) {
      currentFileName = fileName
    }
    setTimeout(() => {
      editorRef.revealPosition({ lineNumber: parseInt(line), column: parseInt(column) ?? 1 })
      window.location.hash = ''
    }, 50)
  })

  const mode = useDarkMode()
  const theme = useSkeletonTheme()
</script>

{@render textEditor(x)}
{#snippet x()}
  <div class="flex h-full flex-col">
    <div class="flex">
      {#each files as file}
        <button
          class="py-1 pl-3 pr-8 {file.name === currentFileName ? 'bg-white-black' : ''}"
          onclick={() => (currentFileName = file.name)}
        >
          {file.name}
        </button>
      {/each}
    </div>
    <div class="relative flex-1">
      <div class="absolute h-full w-full" class:opacity-50={editDisabled}>
        <MonacoEditor
          markers={file.markers}
          on:ready={(x) => {
            x.detail.onKeyDown((e) => {
              if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
                editedFiles[filePath].push()
                e.preventDefault()
              }
            })
          }}
          bind:editor={editorRef}
          bind:value={editedFiles[filePath].current}
          options={{
            fontFamily: theme.config.monospaceFontFamily,
            fontSize: 16,
            theme: mode.darkMode.value === 'light' ? 'vs' : 'vs-dark',
            automaticLayout: true,
            lineNumbersMinChars: 3,
            ...isMonacoEditorDisabled(isReadonly),
            renderValidationDecorations: 'on', // Show red error squiggles even in read-only mode
            overviewRulerLanes: 0,
            hideCursorInOverviewRuler: true,
            overviewRulerBorder: false,
            scrollbar: {
              vertical: 'visible'
            },
            language: file.language
          }}
        />
      </div>
    </div>
  </div>
{/snippet}

<div class="flex flex-wrap items-center gap-x-8 gap-y-2 pr-2">
  <div class="flex h-9 flex-nowrap gap-2">
    <PipelineEditorStatusBar
      {autoSavePipeline}
      downstreamChanged={editedFiles[filePath].downstreamChanged}
      saveCode={editedFiles[filePath].push}
    ></PipelineEditorStatusBar>
    {@render statusBarCenter?.()}
  </div>
  <div class=" ml-auto flex flex-nowrap gap-x-8">
    {@render statusBarEnd?.(editedFiles[filePath].downstreamChanged)}
  </div>
</div>
