<script lang="ts">
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import DeploymentStatus from '$lib/components/pipelines/list/DeploymentStatus.svelte'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { extractProgramError, programErrorReport } from '$lib/compositions/health/systemErrors'
  import { extractSQLCompilerErrorMarkers } from '$lib/functions/pipelines/monaco'
  import {
    postPipelineAction,
    type ExtendedPipeline,
    type Pipeline,
    type PipelineAction
  } from '$lib/services/pipelineManager'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'
  import { nonNull } from '$lib/functions/common/function'
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { useAggregatePipelineStats } from '$lib/compositions/useAggregatePipelineStats.svelte'
  import CodeEditor from '$lib/components/pipelines/editor/CodeEditor.svelte'
  import ProgramStatus from '$lib/components/pipelines/editor/ProgramStatus.svelte'

  let {
    pipeline
  }: {
    pipeline: {
      current: ExtendedPipeline
      patch: (pipeline: Partial<Pipeline>) => Promise<ExtendedPipeline>
      optimisticUpdate: (newPipeline: Partial<ExtendedPipeline>) => Promise<void>
    }
  } = $props()

  let editDisabled = $derived(
    nonNull(pipeline.current.status) && !isPipelineIdle(pipeline.current.status)
  )

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

  let metrics = useAggregatePipelineStats(pipeline, 1000, 61000)
  let files = $derived([
    {
      // name: `${pipeline.current.name}.sql`,
      name: `program.sql`,
      access: {
        get current() {
          return pipeline.current.programCode
        },
        set current(programCode: string) {
          // pipeline.optimisticUpdate({ programCode })
          pipeline.patch({ programCode })
        }
      },
      markers: programErrors ? { sql: extractSQLCompilerErrorMarkers(programErrors) } : undefined
    }
  ])
</script>

<div class="h-full w-full">
  <PaneGroup direction="vertical" class="!overflow-visible">
    <CodeEditor path={pipeline.current.name} {files} {editDisabled}>
      {#snippet textEditor(children)}
        <Pane defaultSize={60} minSize={15} class="!overflow-visible">
          {@render children()}
        </Pane>
        <PaneResizer class="pane-divider-horizontal -mb-0.5" />
      {/snippet}
      {#snippet statusBarCenter()}
        <ProgramStatus programStatus={pipeline.current.programStatus}></ProgramStatus>
      {/snippet}
      {#snippet statusBarEnd(downstreamChanged)}
        {#if pipeline.current.status}
          <DeploymentStatus
            class="ml-auto h-full w-40 text-[1rem] "
            status={pipeline.current.status}
          ></DeploymentStatus>
          <PipelineActions
            {pipeline}
            onDeletePipeline={(pipelineName) =>
              updatePipelines((pipelines) => pipelines.filter((p) => p.name !== pipelineName))}
            pipelineBusy={editDisabled}
            unsavedChanges={downstreamChanged}
            onActionSuccess={(action) => handleActionSuccess(pipeline.current.name, action)}
          ></PipelineActions>
        {/if}
      {/snippet}
    </CodeEditor>
    <div class="h-[1px] w-full bg-surface-100-900"></div>
    <Pane minSize={15} class="flex h-full flex-col !overflow-visible">
      {#if pipeline.current.name}
        <InteractionsPanel {pipeline} {metrics}></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
