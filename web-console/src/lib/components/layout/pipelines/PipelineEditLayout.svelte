<script lang="ts" module>
  let currentPipelineFile: Record<string, string> = $state({})
</script>

<script lang="ts">
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import DeploymentStatus from '$lib/components/pipelines/list/DeploymentStatus.svelte'
  import IconLayputPanelRight from '$assets/icons/generic/layout-panel-right.svg?component'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { base } from '$app/paths'
  import {
    extractProgramErrors,
    programErrorReport,
    programErrorsPerFile
  } from '$lib/compositions/health/systemErrors'
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
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import TabAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'

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
  const handleActionSuccess = async (
    pipelineName: string,
    action: PipelineAction | 'start_paused_start'
  ) => {
    const cbs = pipelineActionCallbacks.getAll(
      pipelineName,
      action === 'start_paused_start' ? 'start_paused' : action
    )
    await Promise.allSettled(cbs.map((x) => x(pipelineName)))
    if (action === 'start_paused_start') {
      postPipelineAction(pipelineName, 'start')
    }
  }
  const handleDeletePipeline = async (pipelineName: string) => {
    updatePipelines((pipelines) => pipelines.filter((p) => p.name !== pipelineName))
    const cbs = pipelineActionCallbacks
      .getAll('', 'delete')
      .concat(pipelineActionCallbacks.getAll(pipelineName, 'delete'))
    cbs.map((x) => x(pipelineName))
  }

  const programErrors = $derived(
    programErrorsPerFile(
      extractProgramErrors(programErrorReport(pipeline.current))({
        name: pipeline.current.name,
        status: pipeline.current.programStatus
      })
    )
  )

  let metrics = useAggregatePipelineStats(pipeline, 1000, 61000)
  let files = $derived.by(() => {
    const current = pipeline.current
    const patch = pipeline.patch
    return [
      {
        name: `program.sql`,
        access: {
          get current() {
            return current.programCode
          },
          set current(programCode: string) {
            patch({ programCode })
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
            return current.programInfo?.udf_stubs ?? ''
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
            return current.programUdfRs
          },
          set current(programUdfRs: string) {
            patch({ programUdfRs })
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
            return current.programUdfToml
          },
          set current(programUdfToml: string) {
            patch({ programUdfToml })
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

  let breadcrumbs = $derived([
    { text: 'Home', href: `${base}/` },
    {
      text: pipelineName,
      href: `${base}/pipelines/${pipelineName}/`
    }
  ])
  let separateAdHocTab = useLocalStorage('layout/pipelines/separateAdHoc', false)
  let downstreamChanged = $state(false)
</script>

<div class="flex h-full flex-col">
  <PipelineBreadcrumbs {breadcrumbs}>
    {#snippet after()}
      <div class="flex items-center">
        <PipelineStatus status={pipeline.current.status}></PipelineStatus>
      </div>
    {/snippet}
    {#snippet end()}
      <PipelineActions
        {pipeline}
        onDeletePipeline={handleDeletePipeline}
        pipelineBusy={editDisabled}
        unsavedChanges={downstreamChanged}
        onActionSuccess={handleActionSuccess}
      ></PipelineActions>
    {/snippet}
  </PipelineBreadcrumbs>
  <PaneGroup direction="vertical" class="">
    <CodeEditor
      path={pipelineName}
      {files}
      {editDisabled}
      bind:currentFileName={currentPipelineFile[pipelineName]}
      bind:downstreamChanged
    >
      {#snippet codeEditor(textEditor, statusBar, isReadOnly)}
        {#snippet editor()}
          <div class="flex h-full flex-col rounded-container px-4 pb-4 pt-2 bg-surface-50-950">
            {@render textEditor()}
            <div
              class:bg-white-black={!isReadOnly}
              class="flex flex-wrap items-center gap-x-8 border-t-[1px] pr-2 border-surface-100-900"
            >
              {@render statusBar()}
            </div>
          </div>
        {/snippet}
        <Pane defaultSize={60} minSize={15} class="!overflow-visible">
          <PaneGroup direction="horizontal" class="">
            <Pane minSize={30}>
              {@render editor()}
            </Pane>
            {#if separateAdHocTab.value}
              <PaneResizer class="pane-divider-vertical mx-2"></PaneResizer>
              <Pane defaultSize={40} minSize={20} class="">
                <div class="flex h-full flex-col rounded-container p-4 bg-surface-50-950">
                  <div class="flex justify-between">
                    <span>Ad-Hoc Queries</span>
                    <button
                      class="fd fd-close btn btn-icon btn-icon-lg"
                      onclick={() => (separateAdHocTab.value = false)}
                      aria-label="Close"
                    ></button>
                  </div>
                  <div class="relative flex-1 overflow-y-auto scrollbar">
                    <div class="absolute left-0 h-full w-full">
                      <TabAdHocQuery {pipeline}></TabAdHocQuery>
                    </div>
                  </div>
                </div>
              </Pane>
            {/if}
          </PaneGroup>
        </Pane>
        <PaneResizer class="pane-divider-horizontal my-2" />
      {/snippet}
      {#snippet statusBarCenter()}
        <ProgramStatus programStatus={pipeline.current.programStatus}></ProgramStatus>
      {/snippet}
      {#snippet toolBarEnd()}
        <button
          class="btn p-2 text-surface-700-300 hover:preset-tonal-surface"
          onclick={() => (separateAdHocTab.value = !separateAdHocTab.value)}
        >
          Ad-Hoc Queries
          <IconLayputPanelRight
            class={separateAdHocTab.value ? 'fill-primary-500' : 'fill-surface-700-300'}
          ></IconLayputPanelRight>
        </button>
      {/snippet}
      {#snippet fileTab(text, onClick, isCurrent)}
        <button
          class="px-3 py-2 {isCurrent
            ? 'inset-y-2 border-b-2 pb-1.5 border-surface-950-50'
            : ' rounded hover:!bg-opacity-50 hover:bg-surface-100-900'}"
          onclick={onClick}
        >
          {text}
        </button>
      {/snippet}
    </CodeEditor>
    <Pane minSize={15} class="flex flex-col !overflow-visible">
      {#if pipeline.current.name}
        <InteractionsPanel {pipeline} {metrics} separateAdHocTab={separateAdHocTab.value}
        ></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
