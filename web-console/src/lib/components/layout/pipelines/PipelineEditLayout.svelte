<script lang="ts">
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import MonacoEditor, { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import PipelineEditorStatusBar from './PipelineEditorStatusBar.svelte'
  import DeploymentStatus from '$lib/components/pipelines/list/DeploymentStatus.svelte'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { useChangedPipelines } from '$lib/compositions/pipelines/useChangedPipelines.svelte'
  import {
    extractProgramError,
    programErrorReport,
    type SystemError
  } from '$lib/compositions/health/systemErrors'
  import { editor } from 'monaco-editor'
  import { extractSQLCompilerErrorMarkers } from '$lib/functions/pipelines/monaco'
  import { page } from '$app/stores'
  import {
    postPipelineAction,
    type ExtendedPipeline,
    type Pipeline,
    type PipelineAction,
    type PipelineStatus as PipelineStatusType
  } from '$lib/services/pipelineManager'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'
  import { nonNull } from '$lib/functions/common/function'
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { useDecoupledState } from '$lib/compositions/decoupledState.svelte'

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)

  let {
    pipeline,
    reloadStatus
  }: {
    pipeline: {
      current: ExtendedPipeline
      patch: (pipeline: Partial<Pipeline>) => Promise<ExtendedPipeline>
      optimisticUpdate: (newPipeline: Partial<ExtendedPipeline>) => Promise<void>
    }
    reloadStatus?: () => void
  } = $props()
  const pipelineCode = {
    get current() {
      return pipeline.current.programCode
    },
    set current(programCode: string) {
      // pipeline.optimisticUpdate({ programCode })
      pipeline.patch({ programCode })
    }
  }

  let wait = $derived(autoSavePipeline.value ? 1000 : ('decoupled' as const))
  let decoupledCode = useDecoupledState(pipelineCode, () => wait)
  {
    // TODO: handle remote update of the program code that conflicts with currently edited version
    let pipelineName = $derived(pipeline.current.name)
    $effect(() => {
      // Fetch new code when switching pipeline
      pipelineName
      setTimeout(() => {
        decoupledCode.pull()
      })
    })
  }

  $effect(() => {
    // Trigger save right away when autosave is turned on
    if (!autoSavePipeline.value) {
      return
    }
    setTimeout(() => decoupledCode.push())
  })

  const changedPipelines = useChangedPipelines()

  $effect(() => {
    if (!pipeline.current.name) {
      return
    }
    decoupledCode.downstreamChanged
      ? changedPipelines.add(pipeline.current.name)
      : changedPipelines.remove(pipeline.current.name)
  })

  {
    let oldPipelineName = $state(pipeline.current.name)
    $effect(() => {
      if (pipeline.current.name === oldPipelineName) {
        return
      }
      changedPipelines.remove(oldPipelineName || '')
      oldPipelineName = pipeline.current.name
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

  const makeStatus = () => ({
    get status() {
      return pipeline.current.status
    },
    set status(status: PipelineStatusType) {
      pipeline.optimisticUpdate({
        status
      })
    }
  })

  let status = $state({ status: makeStatus().status })
  {
    let pipelineName = $derived(pipeline.current.name)
    $effect(() => {
      pipelineName
      status.status = makeStatus().status
    })
  }

  let editDisabled = $derived(nonNull(status) && !isPipelineIdle(status.status))

  const { updatePipelines } = useUpdatePipelineList()

  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const handleActionSuccess = async (pipelineName: string, action: PipelineAction) => {
    const cbs = pipelineActionCallbacks.getAll(pipelineName, action)
    await Promise.allSettled(cbs.map((x) => x()))
    if (action !== 'start_paused') {
      return
    }
    postPipelineAction(pipelineName, 'start')
  }

  const programErrors = $derived(
    extractProgramError(programErrorReport(pipeline.current))({
      name: pipeline.current.name,
      status: pipeline.current.programStatus
    })
  )
  let markers = $state<Record<string, editor.IMarkerData[]>>()
  $effect(() => {
    markers = programErrors ? { sql: extractSQLCompilerErrorMarkers(programErrors) } : undefined
  })
</script>

<div class="h-full w-full">
  <PaneGroup direction="vertical" class="!overflow-visible">
    <Pane defaultSize={60} minSize={15} class="flex flex-col-reverse !overflow-visible">
      <div class="flex flex-nowrap items-center gap-8 pr-2">
        <PipelineEditorStatusBar
          downstreamChanged={decoupledCode.downstreamChanged}
          saveCode={decoupledCode.push}
          programStatus={pipeline.current.programStatus}
        ></PipelineEditorStatusBar>
        {#if status}
          <DeploymentStatus class="ml-auto h-full w-40 text-[1rem] " status={status.status}
          ></DeploymentStatus>
          <PipelineActions
            name={pipeline.current.name}
            bind:status
            {reloadStatus}
            onDeletePipeline={(pipelineName) =>
              updatePipelines((pipelines) => pipelines.filter((p) => p.name !== pipelineName))}
            pipelineBusy={editDisabled}
            unsavedChanges={decoupledCode.downstreamChanged}
            onActionSuccess={(action) => handleActionSuccess(pipeline.current.name, action)}
          ></PipelineActions>
        {/if}
      </div>
      <div class="relative h-full w-full">
        <div class="absolute h-full w-full" class:opacity-50={editDisabled}>
          <MonacoEditor
            {markers}
            on:ready={(x) => {
              x.detail.onKeyDown((e) => {
                if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
                  decoupledCode.push()
                  e.preventDefault()
                }
              })
            }}
            bind:editor={editorRef}
            bind:value={decoupledCode.current}
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
      {#if pipeline.current.name}
        <InteractionsPanel {pipeline}></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
