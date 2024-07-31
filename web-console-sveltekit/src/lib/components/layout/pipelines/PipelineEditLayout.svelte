<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import MonacoEditor, { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import {
    asyncWritable,
    get,
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
    PipelineDescr,
    PipelineStatus as PipelineStatusType
  } from '$lib/services/pipelineManager'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'
  import { nonNull } from '$lib/functions/common/function'

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)

  let {
    pipeline,
    status = $bindable(),
    reloadStatus,
    errors
  }: {
    pipeline: WritableLoadable<PipelineDescr>
    status: { status: PipelineStatusType } | undefined
    reloadStatus?: () => void
    errors?: Readable<SystemError[]>
  } = $props()
  const pipelineCodeStore = asyncWritable(
    pipeline,
    (pipeline) => pipeline.program_code,
    async (newCode, pipeline, oldCode) => {
      if (!pipeline) {
        return oldCode
      }
      $pipeline = {
        ...pipeline,
        program_code: newCode
      }
      return newCode
    },
    { initial: get(pipeline).program_code }
  )

  const decoupledCode = asyncDecoupled(pipelineCodeStore, () =>
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
  let editDisabled = $derived(nonNull(status) && !isPipelineIdle(status.status))
</script>

<div class="h-full w-full">
  <PaneGroup direction="vertical" class="!overflow-visible">
    <Pane defaultSize={60} minSize={15} class="flex flex-col-reverse !overflow-visible">
      <div class="flex flex-nowrap items-center gap-8 pr-2">
        <PipelineEditorStatusBar
          downstreamChanged={decoupledCode.downstreamChanged}
          saveCode={decoupledCode.push}
        ></PipelineEditorStatusBar>
        {#if status}
          <PipelineStatus class="ml-auto h-full w-36 text-[1rem] " status={status.status}
          ></PipelineStatus>
          <PipelineActions
            name={$pipeline.name}
            bind:status
            {reloadStatus}
            pipelineBusy={editDisabled}
            unsavedChanges={decoupledCode.downstreamChanged}
          ></PipelineActions>
        {/if}
      </div>
      <div class="relative h-full w-full">
        <div class="absolute h-full w-full" class:opacity-50={editDisabled}>
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
            }}
          />
        </div>
      </div>
    </Pane>
    <PaneResizer class="h-2 bg-surface-100-900" />
    <Pane minSize={15} class="flex h-full flex-col !overflow-visible">
      {#if $pipeline.name}
        <InteractionsPanel pipelineName={$pipeline.name}></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
