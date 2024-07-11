<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import MonacoEditor, { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import type { Readable, Writable, WritableLoadable } from '@square/svelte-store'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { useDebounce } from 'runed'
  import PipelineEditorStatusBar from './PipelineEditorStatusBar.svelte'
  import PipelineStatus from '$lib/components/pipelines/list/Status.svelte'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { asyncDecoupled } from '$lib/compositions/asyncDecoupled.svelte'
  import { useChangedPipelines } from '$lib/compositions/pipelines/useChangedPipelines.svelte'
  import type { SystemError } from '$lib/compositions/health/systemErrors'
  import { editor } from 'monaco-editor'
  import { extractSQLCompilerErrorMarkers } from '$lib/functions/pipelines/monaco'
  import { page } from '$app/stores'

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)

  const {
    pipelineName,
    pipelineCodeStore,
    errors
  }: {
    pipelineName?: Readable<string>
    pipelineCodeStore: WritableLoadable<string>
    errors?: Readable<SystemError[]>
  } = $props()

  const decoupledCode = asyncDecoupled(
    pipelineCodeStore,
    autoSavePipeline.value ? 1000 : 'decoupled'
  )
  const changedPipelines = useChangedPipelines()
  $effect(() => {
    autoSavePipeline.value ? decoupledCode.debounce(1000) : decoupledCode.decouple()
  })
  $effect(() => {
    if (!decoupledCode.upstreamChanged) {
      return
    }
    decoupledCode.pull()
  })
  $effect(() => {
    if (!$pipelineName) {
      return
    }
    decoupledCode.downstreamChanged
      ? changedPipelines.add($pipelineName)
      : changedPipelines.remove($pipelineName)
  })

  {
    let oldPipelineName = $state($pipelineName)
    $effect(() => {
      if ($pipelineName === oldPipelineName) {
        return
      }
      changedPipelines.remove(oldPipelineName || '')
      oldPipelineName = $pipelineName
    })
  }
  const mode = useDarkMode()

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
</script>

<div class="h-full">
  <PaneGroup direction="vertical" class="!overflow-visible">
    <Pane defaultSize={60} minSize={20} class="flex flex-col-reverse !overflow-visible">
      <div class="flex flex-nowrap items-center gap-2 pr-2">
        <PipelineEditorStatusBar downstreamChanged={decoupledCode.downstreamChanged}
        ></PipelineEditorStatusBar>
        {#if $pipelineName}
          <PipelineStatus class="ml-auto" pipelineName={$pipelineName}></PipelineStatus>
          <PipelineActions pipelineName={$pipelineName}></PipelineActions>
        {/if}
      </div>
      <div class="relative h-full w-full">
        <div class="absolute h-full w-full">
          <MonacoEditor
            markers={$errors ? { sql: extractSQLCompilerErrorMarkers($errors) } : undefined}
            on:ready={(x) => {
              x.detail.onKeyDown((e) => {
                if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
                  decoupledCode.push()
                  e.preventDefault()
                }
              })
            }}
            bind:editor={editorRef}
            bind:value={$decoupledCode}
            options={{
              theme: mode.darkMode.value === 'light' ? 'vs' : 'vs-dark',
              automaticLayout: true,
              lineNumbersMinChars: 3,
              ...isMonacoEditorDisabled(false),
              overviewRulerLanes: 0,
              hideCursorInOverviewRuler: true,
              overviewRulerBorder: false,
              scrollbar: {
                vertical: 'visible'
              },
              language: 'sql'
            }} />
        </div>
      </div>
    </Pane>
    <PaneResizer class="bg-surface-100-900 h-2" />
    <Pane minSize={20} class="!overflow-visible">
      {#if $pipelineName}
        <InteractionsPanel pipelineName={$pipelineName}></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
