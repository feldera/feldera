<script lang="ts" module>
  let currentPipelineFile: Record<string, string> = $state({})
</script>

<script lang="ts">
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import DeploymentStatus from '$lib/components/pipelines/list/DeploymentStatus.svelte'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { extractProgramErrors, programErrorReport } from '$lib/compositions/health/systemErrors'
  import { extractErrorMarkers, felderaCompilerMarkerSource } from '$lib/functions/pipelines/monaco'
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
    extractProgramErrors(programErrorReport(pipeline.current))({
      name: pipeline.current.name,
      status: pipeline.current.programStatus
    })
  )

  let metrics = useAggregatePipelineStats(pipeline, 1000, 61000)
  let files = $derived.by(() => {
    const p = pipeline
    return [
      {
        name: `program.sql`,
        access: {
          get current() {
            return p.current.programCode
          },
          set current(programCode: string) {
            p.patch({ programCode })
          }
        },
        language: 'sql' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['program.sql']
        )
      },
      {
        name: `stubs.rs`,
        access: {
          get current() {
            return p.current.programInfo?.udf_stubs ?? ''
          }
        },
        language: 'rust' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['stubs.rs']
        ),
        behaviorOnConflict: 'auto-pull' as const
      },
      {
        name: `udf.rs`,
        access: {
          get current() {
            return p.current.programUdfRs
          },
          set current(programUdfRs: string) {
            p.patch({ programUdfRs })
          }
        },
        language: 'rust' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['udf.rs']
        ),
        placeholder: `// UDF implementation in Rust.
// See function prototypes in \`stubs.rs\`

pub fn my_udf(input: String) -> Result<String, Box<dyn std::error::Error>> {
  todo!()
}`
      },
      {
        name: `udf.toml`,
        access: {
          get current() {
            return p.current.programUdfToml
          },
          set current(programUdfToml: string) {
            p.patch({ programUdfToml })
          }
        },
        language: 'graphql' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['udf.toml']
        ),
        placeholder: `# List Rust dependencies required by udf.rs.
example = "1.0"`
      }
    ]
  })
  let pipelineName = $derived(pipeline.current.name)
  $effect.pre(() => {
    currentPipelineFile[pipelineName] ??= 'program.sql'
  })
</script>

<div class="h-full w-full">
  <PaneGroup direction="vertical" class="!overflow-visible">
    <CodeEditor
      path={pipelineName}
      {files}
      {editDisabled}
      bind:currentFileName={currentPipelineFile[pipelineName]}
    >
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
          <DeploymentStatus class="ml-auto w-40 text-[1rem] " status={pipeline.current.status}
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
    <Pane minSize={15} class="flex h-full flex-col !overflow-visible">
      {#if pipeline.current.name}
        <InteractionsPanel {pipeline} {metrics}></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
