<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import MonacoEditor, { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import {
    asyncWritable,
    type Readable,
    type Writable,
    type WritableLoadable
  } from '@square/svelte-store'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { Store, useDebounce } from 'runed'
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
  import type {
    FullPipeline,
    PipelineStatus as PipelineStatusType
  } from '$lib/services/pipelineManager'
  import { pipelineStats } from '$lib/services/manager'
  import { match } from 'ts-pattern'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)

  const {
    pipeline,
    errors,
    status
  }: {
    pipeline: WritableLoadable<FullPipeline>
    status: PipelineStatusType | undefined
    errors?: Readable<SystemError[]>
  } = $props()

  const pipelineCodeStore = asyncWritable(
    pipeline,
    (pipeline) => pipeline.code,
    async (newCode, pipeline, oldCode) => {
      if (!pipeline || !newCode) {
        return oldCode
      }
      $pipeline = {
        ...pipeline,
        code: newCode
      }
      return newCode
    }
  )

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
    if (!$pipeline.name) {
      return
    }
    decoupledCode.downstreamChanged
      ? changedPipelines.add($pipeline.name)
      : changedPipelines.remove($pipeline.name)
  })

  {
    let oldPipelineName = $state($pipeline.name)
    $effect(() => {
      if ($pipeline.name === oldPipelineName) {
        return
      }
      changedPipelines.remove(oldPipelineName || '')
      oldPipelineName = $pipeline.name
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
  let editDisabled = $derived(status && !isPipelineIdle(status))
</script>

<div class="h-full">
  <PaneGroup direction="vertical" class="!overflow-visible">
    <Pane defaultSize={60} minSize={20} class="flex flex-col-reverse !overflow-visible">
      <div class="flex flex-nowrap items-center gap-2 pr-2">
        <PipelineEditorStatusBar downstreamChanged={decoupledCode.downstreamChanged}
        ></PipelineEditorStatusBar>
        {#if status}
          <PipelineStatus class="ml-auto" {status}></PipelineStatus>

          <PipelineActions name={$pipeline.name} {status}></PipelineActions>
        {/if}
      </div>
      <div class="relative h-full w-full">
        <div class="absolute h-full w-full" class:opacity-70={editDisabled}>
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
              ...isMonacoEditorDisabled(editDisabled),
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
      {#if $pipeline.name}
        <InteractionsPanel pipelineName={$pipeline.name}></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
