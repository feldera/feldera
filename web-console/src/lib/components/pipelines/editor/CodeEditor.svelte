<script lang="ts">
  import type { Snippet } from 'svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { useDecoupledState } from '$lib/compositions/decoupledState.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import MonacoEditor, { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import { editor } from 'monaco-editor'
  import PipelineEditorStatusBar from '$lib/components/layout/pipelines/PipelineEditorStatusBar.svelte'
  import { page } from '$app/stores'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'

  let {
    path,
    files,
    editDisabled,
    textEditor,
    statusBarCenter,
    statusBarEnd
  }: {
    path: string
    files: {
      name: string
      access: { current: string }
      markers?: Record<string, editor.IMarkerData[]>
    }[]
    editDisabled?: boolean
    textEditor: Snippet<[children: Snippet]>
    statusBarCenter?: Snippet
    statusBarEnd?: Snippet<[downstreamChanged: boolean]>
  } = $props()

  let currentFileName = $state(files[0].name)

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)

  let wait = $derived(autoSavePipeline.value ? 2000 : ('decoupled' as const))
  let file = $derived(files.find((f) => f.name === currentFileName)!)
  let editedText = useDecoupledState(file.access, () => wait)
  {
    // TODO: handle remote update of the program code that conflicts with currently edited version
    let pipelineName = $derived(path)
    $effect(() => {
      // Fetch new code when switching pipeline
      pipelineName
      setTimeout(() => {
        editedText.pull()
      })
    })
  }

  $effect(() => {
    // Trigger save right away when autosave is turned on
    if (!autoSavePipeline.value) {
      return
    }
    setTimeout(() => editedText.push())
  })

  let editorRef: editor.IStandaloneCodeEditor = $state()!
  $effect(() => {
    if (!editorRef) {
      return
    }
    const [, line, , column] = $page.url.hash.match(/#:(\d+)(:(\d+))?/) ?? []
    if (!line) {
      return
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
        <div class="py-1 pl-3 pr-8 {file.name === currentFileName ? 'bg-white-black' : ''}">
          {file.name}
        </div>
      {/each}
    </div>
    <div class="relative flex-1">
      <div class="absolute h-full w-full" class:opacity-50={editDisabled}>
        <MonacoEditor
          markers={file.markers}
          on:ready={(x) => {
            x.detail.onKeyDown((e) => {
              if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
                editedText.push()
                e.preventDefault()
              }
            })
          }}
          bind:editor={editorRef}
          bind:value={editedText.current}
          options={{
            fontFamily: theme.config.monospaceFontFamily,
            fontSize: 16,
            theme: mode.darkMode.value === 'light' ? 'vs' : 'vs-dark',
            automaticLayout: true,
            lineNumbersMinChars: 3,
            ...isMonacoEditorDisabled(editDisabled),
            overviewRulerLanes: 0,
            hideCursorInOverviewRuler: true,
            overviewRulerBorder: false,
            scrollbar: {
              vertical: 'visible'
            },
            language: 'sql'
          }}
        />
      </div>
    </div>
  </div>
{/snippet}

<div class="flex flex-nowrap items-center gap-8 pr-2">
  <div class="flex h-full flex-nowrap gap-2">
    <PipelineEditorStatusBar
      {autoSavePipeline}
      downstreamChanged={editedText.downstreamChanged}
      saveCode={editedText.push}
    ></PipelineEditorStatusBar>
    {@render statusBarCenter?.()}
  </div>
  {@render statusBarEnd?.(editedText.downstreamChanged)}
</div>
