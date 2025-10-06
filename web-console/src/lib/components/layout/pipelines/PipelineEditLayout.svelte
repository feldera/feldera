<script lang="ts" module>
  let currentPipelineFile: Record<string, string> = $state({})
</script>

<script lang="ts">
  import { PaneGroup, Pane, PaneResizer, type PaneAPI } from 'paneforge'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { base } from '$app/paths'
  import {
    extractProgramErrors,
    programErrorReport,
    programErrorsPerFile
  } from '$lib/compositions/health/systemErrors'
  import { extractErrorMarkers, felderaCompilerMarkerSource } from '$lib/functions/pipelines/monaco'
  import {
    programStatusOf,
    type ExtendedPipeline,
    type Pipeline,
    type PipelineAction,
    type PipelineThumb
  } from '$lib/services/pipelineManager'
  import { isPipelineCodeEditable, isPipelineConfigEditable } from '$lib/functions/pipelines/status'
  import { nonNull } from '$lib/functions/common/function'
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { useAggregatePipelineStats } from '$lib/compositions/useAggregatePipelineStats.svelte'
  import CodeEditor from '$lib/components/pipelines/editor/CodeEditor.svelte'
  import ProgramStatus from '$lib/components/pipelines/editor/ProgramStatus.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import TabAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import EditorOptionsPopup from './EditorOptionsPopup.svelte'
  import { useIsTablet, useIsScreenLg } from '$lib/compositions/layout/useIsMobile.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import PipelineList from '$lib/components/pipelines/List.svelte'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import DoubleClickInput from '$lib/components/input/DoubleClickInput.svelte'
  import { goto } from '$app/navigation'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
  import PipelineBanner from '$lib/components/pipelines/editor/PipelineBanner.svelte'
  import { useContextDrawer } from '$lib/compositions/layout/useContextDrawer.svelte'
  import ReviewPipelineChanges from '$lib/components/pipelines/editor/ReviewPipelineChangesDialog.svelte'
  import { parsePipelineDiff } from '$lib/functions/pipelines/pipelineDiff'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { usePipelineAction } from '$lib/compositions/usePipelineAction.svelte'

  let {
    preloaded,
    pipeline
  }: {
    preloaded: { pipelines: PipelineThumb[] }
    pipeline: WritablePipeline
  } = $props()

  let editCodeDisabled = $derived(
    (nonNull(pipeline.current.status) && !isPipelineCodeEditable(pipeline.current.status)) ||
      (pipeline.current.storageStatus !== 'Cleared' &&
        !pipeline.current.runtimeConfig?.dev_tweaks?.['backfill_avoidance'])
  )
  let editConfigDisabled = $derived(
    nonNull(pipeline.current.status) && !isPipelineConfigEditable(pipeline.current.status)
  )

  const { updatePipelines, updatePipeline } = useUpdatePipelineList()
  const pipelineAction = usePipelineAction()

  const api = usePipelineManager()
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const handleActionSuccess = async (pipelineName: string, action: PipelineAction) => {
    const cbs = pipelineActionCallbacks.getAll(pipelineName, action)
    await Promise.allSettled(cbs.map((x) => x(pipelineName)))
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
      extractProgramErrors(programErrorReport(pipeline.current))(pipeline.current)
    )
  )

  let metrics = useAggregatePipelineStats(pipeline, 2000, 63000)

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
            patch({ programCode }, true)
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
            patch({ programUdfRs }, true)
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
            patch({ programUdfToml }, true)
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

  const isTablet = useIsTablet()
  const isScreenLg = useIsScreenLg()
  const drawer = useAdaptiveDrawer('right')
  const pipelineList = usePipelineList(preloaded)

  let { showPipelinesPanel, showMonitoringPanel, separateAdHocTab } = useLayoutSettings()
  let downstreamChanged = $state(false)
  let isDraggingPipelineListResizer = $state(false)
  let saveFile = $state(() => {})

  let pipelineListPane = $state<PaneAPI>()
  $effect(() => {
    if (!pipelineListPane) {
      return
    }
    if (showPipelinesPanel.value) {
      // pipelineListPane.expand()
    } else {
      pipelineListPane.collapse()
    }
  })
  $effect(() => {
    if (!pipelineListPane) {
      return
    }
    void showPipelinesPanel.value
    if (isTablet.current) {
      pipelineListPane.collapse()
    } else if (showPipelinesPanel.value) {
      pipelineListPane.expand()
    }
  })
  $effect(() => {
    if (!isScreenLg.current) {
      separateAdHocTab.value = false
    }
  })

  const contextDrawer = useContextDrawer()

  let pipelineBannerMessage = $derived.by(() => {
    if (pipeline.current.deploymentError) {
      return {
        header: `The last execution of the pipeline failed with the error code: ${pipeline.current.deploymentError.error_code}`,
        message: pipeline.current.deploymentError.message,
        style: 'error' as const
      }
    } else if (pipeline.current.status === 'AwaitingApproval') {
      return {
        header:
          'The pipeline was modified while it was stopped. Approve the changes or stop the pipeline.',
        style: 'warning' as const,
        actions: [
          {
            label: 'Review Changes',
            onclick: () => {
              contextDrawer.content = reviewPipelineChanges
            }
          }
        ]
      }
    }
    return null
  })

  let toast = useToast()
  let safeParsePipelineDiff = (pipeline: ExtendedPipeline) => {
    if (pipeline.status !== 'AwaitingApproval') {
      return undefined
    }
    try {
      return parsePipelineDiff(pipeline)
    } catch (e) {
      if (e instanceof Error) {
        setTimeout(() => {
          contextDrawer.content = null
          toast.toastError(e)
        })
      }
      return undefined
    }
  }
</script>

{#snippet reviewPipelineChanges()}
  {@const changes = safeParsePipelineDiff(pipeline.current)}
  {#if changes}
    <ReviewPipelineChanges
      {changes}
      onCancel={() => (contextDrawer.content = null)}
      onApprove={async () => {
        const { waitFor } = await pipelineAction.postPipelineAction(
          pipeline.current.name,
          'approve_changes'
        )
        await waitFor()
      }}
    >
      {#snippet titleEnd()}
        <button
          class="fd fd-x btn btn-icon text-[24px]"
          aria-label="Close"
          onclick={() => (contextDrawer.content = null)}
        ></button>
      {/snippet}
    </ReviewPipelineChanges>
  {/if}
{/snippet}

{#snippet pipelineActions(props?: { class: string })}
  <PipelineActions
    class={props?.class}
    {pipeline}
    onDeletePipeline={handleDeletePipeline}
    {editConfigDisabled}
    unsavedChanges={downstreamChanged}
    onActionSuccess={handleActionSuccess}
    {saveFile}
  ></PipelineActions>
{/snippet}

<div class="flex h-full w-full flex-col">
  <AppHeader>
    {#snippet afterStart()}
      <div class="flex min-w-0 flex-1 flex-col gap-x-4 gap-y-1 sm:flex-row sm:items-center">
        <PipelineBreadcrumbs
          class="-ml-3 pb-1 pl-3"
          textClass="text-base"
          breadcrumbs={[
            ...(isTablet.current
              ? []
              : [
                  {
                    text: 'Home',
                    href: `${base}/`
                  }
                ])
          ]}
        >
          {#snippet last()}
            <DoubleClickInput
              value={pipeline.current.name}
              onvalue={(name) => {
                if (name === pipeline.current.name) {
                  return
                }
                const newUrl = `${base}/pipelines/${encodeURIComponent(name)}/`
                return pipeline.patch({ name }).then(() => {
                  goto(newUrl, { replaceState: true })
                })
              }}
              disabled={editCodeDisabled}
              class="inline overflow-hidden overflow-ellipsis"
              inputClass="input flex -ml-1 mr-2 py-0 pl-1 text-base mt-1"
            >
              <span class="text-base">
                {pipeline.current.name}
              </span>
            </DoubleClickInput>
            {#if editCodeDisabled}
              <Tooltip class="z-10 rounded bg-white text-base text-surface-950-50 dark:bg-black">
                Cannot edit the pipeline's name while it's running
              </Tooltip>
            {/if}
          {/snippet}
        </PipelineBreadcrumbs>
        <PipelineStatus class="h-6" status={pipeline.current.status}></PipelineStatus>
      </div>
    {/snippet}
    {#snippet beforeEnd()}
      {#if drawer.isMobileDrawer}
        <button
          onclick={() => (drawer.value = !drawer.value)}
          class="fd fd-book-open btn-icon flex text-[20px] preset-tonal-surface"
          aria-label="Open extras drawer"
        >
        </button>
      {:else}
        <NavigationExtras></NavigationExtras>
        <div class="relative">
          <CreatePipelineButton inputClass="max-w-64" btnClass="preset-filled-surface-50-950"
          ></CreatePipelineButton>
        </div>
        <BookADemo class="btn-icon preset-filled-surface-50-950"></BookADemo>
        <Tooltip class="bg-white-dark z-10 rounded text-surface-950-50">Book a demo</Tooltip>
      {/if}
    {/snippet}
  </AppHeader>
  <PaneGroup direction="horizontal" class="!overflow-visible px-2 pb-4 md:pl-8 md:pr-8 xl:pl-4">
    <Pane
      defaultSize={15}
      minSize={10}
      class="relative h-full"
      bind:pane={pipelineListPane}
      collapsible
      onCollapse={() => {
        if (showPipelinesPanel.value && !isTablet.current) {
          showPipelinesPanel.value = false
        }
      }}
      onExpand={() => {
        if (!isDraggingPipelineListResizer) {
          return
        }
        if (!showPipelinesPanel.value) {
          showPipelinesPanel.value = true
        }
      }}
    >
      <div class="absolute flex h-full w-full flex-col">
        <PipelineList
          pipelines={pipelineList.pipelines}
          onclose={() => {
            showPipelinesPanel.value = false
          }}
        ></PipelineList>
      </div>
    </Pane>
    {#if !isTablet.current}
      <PaneResizer
        class="pane-divider-vertical ml-1.5 mr-2"
        onDraggingChange={(isDragging: boolean) => {
          isDraggingPipelineListResizer = isDragging
        }}
      ></PaneResizer>
    {/if}

    <Pane class="!overflow-visible">
      <PaneGroup direction="vertical" class="!overflow-visible">
        {#if pipelineBannerMessage}
          <div class="pb-2 md:pb-4">
            <PipelineBanner
              header={pipelineBannerMessage.header}
              message={pipelineBannerMessage.message}
              actions={pipelineBannerMessage.actions ?? []}
              style={pipelineBannerMessage.style}
            ></PipelineBanner>
          </div>
        {/if}
        <CodeEditor
          path={pipelineName}
          {files}
          editDisabled={editCodeDisabled}
          bind:currentFileName={currentPipelineFile[pipelineName]}
          bind:downstreamChanged
          bind:saveFile
        >
          {#snippet codeEditor(textEditor, statusBar)}
            {#snippet editor()}
              <div class="flex h-full flex-col rounded-container px-4 py-2 bg-surface-50-950">
                {@render textEditor()}
                <div
                  class="bg-white-dark mb-2 flex flex-wrap items-center gap-x-8 rounded-b border-t p-2 pl-4 border-surface-50-950"
                >
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
                          class="fd fd-x btn btn-icon btn-icon-lg !h-6"
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
          {/snippet}
          {#snippet toolBarEnd()}
            <div class="flex justify-end gap-4 pb-2">
              {@render pipelineActions()}
            </div>
          {/snippet}
          {#snippet statusBarCenter()}
            <ProgramStatus programStatus={programStatusOf(pipeline.current.status)}></ProgramStatus>
          {/snippet}
          {#snippet statusBarEnd()}
            <div class="ml-auto flex flex-nowrap items-center gap-1">
              {#each [{ icon: 'fd fd-panel-left', text: 'Pipelines', value: showPipelinesPanel }, { icon: 'fd fd-panel-bottom', text: 'Monitoring', value: showMonitoringPanel }, { icon: 'fd fd-panel-right', text: 'Ad-Hoc Queries', value: separateAdHocTab, show: isScreenLg.current }] as { icon, text, value, show }}
                {#if show !== false}
                  <button
                    class="btn gap-2 p-2 !brightness-100 text-surface-700-300 hover:preset-tonal-surface"
                    onclick={() => (value.value = !value.value)}
                  >
                    <span class="hidden sm:inline">
                      {text}
                    </span>
                    <div class="{icon} text-[20px] {value.value ? 'text-primary-500' : ''}"></div>
                  </button>
                  <div class="pointer-events-none w-0 -translate-x-0.5">|</div>
                {/if}
              {/each}
            </div>
            <EditorOptionsPopup></EditorOptionsPopup>
          {/snippet}
          {#snippet fileTab(text, onClick, isCurrent, isSaved)}
            <button
              class=" flex flex-nowrap py-2 pl-2 pr-5 font-medium sm:pl-3 {isCurrent
                ? 'inset-y-2 border-b-2 pb-1.5 border-surface-950-50'
                : ' rounded hover:!bg-opacity-50 hover:bg-surface-100-900'}"
              onclick={onClick}
            >
              {text}
              <div
                class="h-0 w-0 -translate-x-2 -translate-y-1.5 text-4xl {isSaved
                  ? ''
                  : 'fd fd-dot'}"
              ></div>
            </button>
          {/snippet}
        </CodeEditor>
        {#if showMonitoringPanel.value && pipeline.current.name}
          <PaneResizer class="pane-divider-horizontal my-2" />
          <Pane minSize={15} class="flex flex-col !overflow-visible">
            <InteractionsPanel {pipeline} {metrics} separateAdHocTab={separateAdHocTab.value}
            ></InteractionsPanel>
          </Pane>
        {/if}
      </PaneGroup>
    </Pane>
  </PaneGroup>
</div>
