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
    type PipelineAction,
    type PipelineThumb
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
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import PipelineToolbarMenu from '$lib/components/layout/pipelines/PipelineToolbarMenu.svelte'
  import PipelineListPopup from './PipelineListPopup.svelte'
  import EditorOptionsPopup from './EditorOptionsPopup.svelte'
    import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'

  let {
    preloaded,
    pipeline
  }: {
    preloaded: { pipelines: PipelineThumb[] }
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

  let separateAdHocTab = useLocalStorage('layout/pipelines/separateAdHoc', false)
  let downstreamChanged = $state(false)
  let saveFile = $state(() => {})

  const isTablet = useIsTablet()
</script>

{#snippet pipelineActions(props?: { class: string })}
  <div class={props?.class}>
    <EditorOptionsPopup></EditorOptionsPopup>
  </div>
  <div class={props?.class}>
    <PipelineToolbarMenu
      {pipeline}
      {pipelineName}
      {saveFile}
      pipelineBusy={editDisabled}
      {downstreamChanged}
      onDeletePipeline={handleDeletePipeline}
    ></PipelineToolbarMenu>
  </div>
  <PipelineActions
    class={props?.class}
    {pipeline}
    onDeletePipeline={handleDeletePipeline}
    pipelineBusy={editDisabled}
    unsavedChanges={downstreamChanged}
    onActionSuccess={handleActionSuccess}
  ></PipelineActions>
{/snippet}

<div class="flex h-full w-full flex-col">
  <AppHeader>
    <div class="flex flex-col gap-x-4 gap-y-1 md:flex-row-reverse lg:mt-0">
      <PipelineStatus status={pipeline.current.status}></PipelineStatus>
      <PipelineListPopup {preloaded}>
        {#snippet trigger(toggle)}
          <PipelineBreadcrumbs
            breadcrumbs={[
              ...isTablet.current ? [] : [{
                text: 'Home',
                href: `${base}/`
              }],
              {
                text: pipelineName,
                onclick: toggle
              }
            ]}
          ></PipelineBreadcrumbs>
        {/snippet}
      </PipelineListPopup>
    </div>
    {#snippet beforeEnd()}
      {@render pipelineActions({ class: 'hidden lg:flex' })}
    {/snippet}

    <!--  -->
  </AppHeader>
  <div class="flex w-full justify-end gap-4 px-2 pb-4 md:px-8 lg:hidden">
    {@render pipelineActions()}
  </div>
  <PaneGroup direction="vertical" class="!overflow-visible px-2 pb-4 md:px-8">
    <CodeEditor
      path={pipelineName}
      {files}
      {editDisabled}
      bind:currentFileName={currentPipelineFile[pipelineName]}
      bind:downstreamChanged
      bind:saveFile
    >
      {#snippet codeEditor(textEditor, statusBar)}
        {#snippet editor()}
          <div class="flex h-full flex-col rounded-container px-4 py-2 bg-surface-50-950">
            {@render textEditor()}
            <div class="flex flex-wrap items-center gap-x-8 pt-2 border-surface-100-900">
              {@render statusBar()}
            </div>
          </div>
        {/snippet}
        <Pane defaultSize={60} minSize={15} class="!overflow-visible">
          <PaneGroup direction="horizontal" class="!overflow-visible">
            <Pane minSize={30} class="!overflow-visible">
              {@render editor()}
            </Pane>
            {#if separateAdHocTab.value}
              <PaneResizer class="pane-divider-vertical mx-2"></PaneResizer>
              <Pane defaultSize={40} minSize={20} class="!overflow-visible">
                <div class="flex h-full flex-col rounded-container p-4 bg-surface-50-950">
                  <div class="flex h-8 items-start justify-between">
                    <span>Ad-Hoc Queries</span>
                    <button
                      class="fd fd-close btn btn-icon btn-icon-lg !h-6"
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
      {#snippet toolBarEnd()}
        <div class="ml-auto flex flex-nowrap items-center gap-2">
          <button
            class="btn hidden p-2 text-surface-700-300 hover:preset-tonal-surface lg:flex"
            onclick={() => (separateAdHocTab.value = !separateAdHocTab.value)}
          >
            {#if !separateAdHocTab.value}
              Ad-Hoc Queries
            {/if}
            <IconLayputPanelRight
              class={separateAdHocTab.value ? 'fill-primary-500' : 'fill-surface-700-300'}
            ></IconLayputPanelRight>
          </button>
        </div>
      {/snippet}
      {#snippet statusBarCenter()}
        <ProgramStatus programStatus={pipeline.current.programStatus}></ProgramStatus>
      {/snippet}
      {#snippet statusBarEnd()}{/snippet}
      {#snippet fileTab(text, onClick, isCurrent)}
        <button
          class="px-2 py-2 sm:px-3 {isCurrent
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
