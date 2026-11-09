<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { singleton } from 'common-lib/array'
  import { nonNull } from 'common-lib/function'
  import { Tooltip, useIsScreenLg, useIsTablet } from 'common-ui'
  import { Pane, type PaneAPI, PaneGroup, PaneResizer } from 'paneforge'
  import { untrack } from 'svelte'
  import DoubleClickInput from '#lib/components/input/DoubleClickInput.svelte'
  import AppHeader from '#lib/components/layout/AppHeader.svelte'
  import NavigationExtras from '#lib/components/layout/NavigationExtras.svelte'
  import PipelineBreadcrumbs from '#lib/components/layout/PipelineBreadcrumbs.svelte'
  import BookADemo from '#lib/components/other/BookADemo.svelte'
  import CreatePipelineButton from '#lib/components/pipelines/CreatePipelineButton.svelte'
  import InteractionPanel from '#lib/components/pipelines/editor/InteractionPanel.svelte'
  import MonitoringPanel, {
    type MonitoringTabs
  } from '#lib/components/pipelines/editor/MonitoringPanel.svelte'
  import PipelineBanner from '#lib/components/pipelines/editor/PipelineBanner.svelte'
  import PipelineVersion from '#lib/components/pipelines/editor/PipelineVersion.svelte'
  import ProgramStatus from '#lib/components/pipelines/editor/ProgramStatus.svelte'
  import ReviewPipelineChanges from '#lib/components/pipelines/editor/ReviewPipelineChangesDialog.svelte'
  import * as TabPerformance from '#lib/components/pipelines/editor/TabPerformance.svelte'
  import PipelineList from '#lib/components/pipelines/List.svelte'
  import PipelineStatus from '#lib/components/pipelines/list/PipelineStatus.svelte'
  import PipelineTransactionStatus from '#lib/components/pipelines/list/PipelineTransactionStatus.svelte'
  import { useAdaptiveDrawer } from '#lib/compositions/layout/useAdaptiveDrawer.svelte.ts'
  import { useContextDrawer } from '#lib/compositions/layout/useContextDrawer.svelte.ts'
  import { useLayoutSettings } from '#lib/compositions/layout/useLayoutSettings.svelte.ts'
  import { usePipelineActionCallbacks } from '#lib/compositions/pipelines/usePipelineActionCallbacks.svelte.ts'
  import {
    PIPELINE_STATS_POLL_MS,
    useAggregatePipelineStats
  } from '#lib/compositions/useAggregatePipelineStats.svelte.ts'
  import { usePermission } from '#lib/compositions/usePermission.svelte.ts'
  import { getPipelineAction } from '#lib/compositions/usePipelineAction.svelte.ts'
  import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
  import { useToast } from '#lib/compositions/useToastNotification.ts'
  import type { WritablePipeline } from '#lib/compositions/useWritablePipeline.svelte.ts'
  import type { PipelineMetrics } from '#lib/functions/pipelineMetrics.ts'
  import { parsePipelineDiff } from '#lib/functions/pipelines/pipelineDiff.ts'
  import { isPipelineCodeEditable } from '#lib/functions/pipelines/status.ts'
  import { resolve } from '#lib/functions/svelte.ts'
  import {
    type ExtendedPipeline,
    type PipelineThumb,
    programStatusOf
  } from '#lib/services/pipelineManager.ts'
  import { goto } from '$app/navigation'
  import { page } from '$app/state'
  import EditorOptionsPopup from './EditorOptionsPopup.svelte'
  import PipelineCodePanel from './PipelineCodePanel.svelte'

  let {
    pipelineName,
    pipelineList,
    pipelineThumb,
    pipeline,
    deleted = false
  }: {
    pipelineName: string
    pipelineList: { pipelines: PipelineThumb[] | undefined } // If it's undefined - it is loading
    pipelineThumb: PipelineThumb | undefined // If it's undefined - it is loading
    pipeline: WritablePipeline // If .current is undefined - it is loading
    deleted?: boolean
  } = $props()

  // Renaming needs write:pipeline_meta; without it the name stays visible but
  // the double-click-to-edit affordance is inert.
  const canRename = usePermission('write:pipeline_meta')
  // Dismissing a deployment error acts on the pipeline, so it needs exec:pipeline.
  const canDismissError = usePermission('exec:pipeline')

  let editNameDisabled = $derived(
    !canRename.allowed ||
      !pipelineThumb ||
      deleted ||
      (nonNull(pipelineThumb.status) && !isPipelineCodeEditable(pipelineThumb.status))
  )

  const api = usePipelineManager()
  const pipelineAction = getPipelineAction()
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  let metrics = useAggregatePipelineStats(pipeline, PIPELINE_STATS_POLL_MS, 63000, {
    getDeleted: () => deleted
  })

  const isTablet = useIsTablet()
  const isScreenLg = useIsScreenLg()
  const drawer = useAdaptiveDrawer('right')

  let { showPipelinesPanel, showMonitoringPanel, showInteractionPanel } = useLayoutSettings()
  let isDraggingPipelineListResizer = $state(false)

  let pipelineListPane = $state<PaneAPI>()
  $effect(() => {
    if (!pipelineListPane) {
      return
    }
    if (!showPipelinesPanel.value) {
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
      showInteractionPanel.value = false
    }
  })

  const contextDrawer = useContextDrawer()

  let pipelineBannerMessage = $derived.by(() => {
    if (!pipelineThumb) {
      return null
    }
    if (deleted) {
      return {
        header: 'This pipeline has been deleted',
        message: 'All editing and monitoring has been disabled.',
        style: 'error' as const
      }
    }
    if (pipelineThumb.deploymentError) {
      return {
        header: `The last execution of the pipeline failed with the error code: ${pipelineThumb.deploymentError.error_code}`,
        message: pipelineThumb.deploymentError.message,
        style: 'error' as const,
        onClose: canDismissError.allowed
          ? () => pipelineThumb && api.dismissDeploymentError(pipelineThumb.name)
          : undefined
      }
    } else if (pipelineThumb.status === 'AwaitingApproval') {
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
          toast.toastError('Parsing pipeline diff')(e)
        })
      }
      return undefined
    }
  }

  let currentMonitoringTab: MonitoringTabs | null = $state(null)
  // Reported by the code panel, so the Runtime tab's Start button can refuse to start an
  // unsaved program, like the toolbar's Start does.
  let unsavedChanges = $state(false)
  let currentInteractionTab: string | null = $state(null)

  const monitoringTabSwitchTo = async () => {
    if (currentMonitoringTab === 'Errors') {
      currentMonitoringTab = 'Performance'
    }
  }
  $effect(() => {
    pipelineName
    untrack(() => pipelineActionCallbacks.add(pipelineName, 'start', monitoringTabSwitchTo))
    return () => {
      pipelineActionCallbacks.remove(pipelineName, 'start', monitoringTabSwitchTo)
    }
  })

  let layoutControls = $derived([
    { icon: 'fd fd-panel-bottom', text: 'Monitoring', value: showMonitoringPanel },
    {
      icon: 'fd fd-panel-right',
      text: 'Inspect',
      value: showInteractionPanel,
      show: isScreenLg.current
    }
  ])

  const sideGutters = 'px-2 md:px-5'
</script>

{#snippet reviewPipelineChanges()}
  {#if pipeline.current}
    {@const changes = safeParsePipelineDiff(pipeline.current)}
    {#if changes}
      <ReviewPipelineChanges
        {changes}
        onCancel={() => (contextDrawer.content = null)}
        onApprove={async () => {
          const { waitFor } = await pipelineAction.postPipelineAction(
            pipelineName,
            'approve_changes'
          )
          await waitFor()
        }}
      >
        {#snippet titleEnd()}
          <button
            class="fd fd-x btn-icon btn text-[20px]"
            aria-label="Close"
            onclick={() => (contextDrawer.content = null)}
          ></button>
        {/snippet}
      </ReviewPipelineChanges>
    {/if}
  {/if}
{/snippet}

<div class="flex h-full w-full flex-col">
  <AppHeader class={sideGutters}>
    {#snippet afterStart()}
      <!-- One row that wraps: the status chip drops under the name only when they don't fit. -->
      <div class="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1">
        <PipelineBreadcrumbs
          class="-ml-3 items-center py-1 pl-3"
          textClass="text-base"
          breadcrumbs={[
            ...(isTablet.current
              ? []
              : [
                  {
                    text: 'Home',
                    href: resolve('/')
                  }
                ])
          ]}
        >
          {#snippet last()}
            <DoubleClickInput
              value={pipelineName}
              onvalue={(name) => {
                if (name === pipelineName) {
                  return
                }
                const newUrl = resolve(`/pipelines/${encodeURIComponent(name)}/`)
                return pipeline.patch({ name }).then(() => {
                  goto(newUrl, { replace: true })
                })
              }}
              disabled={editNameDisabled}
              editLabel="Edit pipeline name"
              class="flex min-w-0 items-center gap-1"
              inputClass="input flex -ml-1 mr-2 py-0 pl-1 text-base"
            >
              <span class="truncate text-base font-medium">
                {pipelineName}
              </span>
            </DoubleClickInput>
            {#if editNameDisabled}
              <Tooltip class="">
                {#if deleted}
                  Cannot edit the deleted pipeline's name
                {:else if !canRename.allowed}
                  You have read-only access
                {:else}
                  Cannot edit the pipeline's name while it's running
                {/if}
              </Tooltip>
            {/if}
          {/snippet}
        </PipelineBreadcrumbs>
        {#if pipelineThumb}
          <PipelineStatus
            data-testid="box-pipeline-status"
            class="shrink-0"
            status={pipelineThumb.status}
            {deleted}
          ></PipelineStatus>
        {/if}
        {#if metrics.current}
          <PipelineTransactionStatus
            globalMetrics={metrics.current.global}
            onClick={() => {
              currentMonitoringTab = TabPerformance.id
            }}
          ></PipelineTransactionStatus>
        {/if}
      </div>
    {/snippet}
    {#snippet beforeEnd()}
      {#if drawer.isMobileDrawer}
        <button
          onclick={() => (drawer.value = !drawer.value)}
          class="fd fd-book-open btn-icon flex preset-tonal-surface text-[16px]"
          aria-label="Open the right navigation drawer"
        >
        </button>
      {:else}
        <NavigationExtras></NavigationExtras>
        <div class="relative">
          <CreatePipelineButton inputClass="max-w-64" btnClass="preset-filled-surface-50-950"
          ></CreatePipelineButton>
        </div>
        <BookADemo class="btn-icon preset-filled-surface-50-950" placement="pipeline_editor"
        ></BookADemo>
        <Tooltip class="">Book a demo</Tooltip>
      {/if}
    {/snippet}
  </AppHeader>
  <!-- The areas sit flush against each other, with no gaps, card corners or card borders:
       the header's bottom line and the 1px pane dividers are the only separators. `!` beats
       the `.panel` corners and border, which other pages keep. The tabbed panels drop their
       side padding so the tab bar runs edge to edge, and pad the tab content instead. In dark mode the dividers between the panels
       take the panels' own line colour, since the translucent line vanishes between them. -->
  <PaneGroup
    direction="horizontal"
    class="!overflow-visible [&_.panel]:rounded-none! [&_.panel]:border-0! [&_.panel>[role=tablist]]:border-line [&_.panel>[role=tablist]+div]:mx-2 [&_.panel[data-scope=tabs]]:px-0!"
  >
    <Pane
      defaultSize={15}
      minSize={10}
      class="relative h-full"
      bind:this={pipelineListPane}
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
      <div class="absolute flex h-full w-full flex-col pl-2 md:pl-3">
        <PipelineList
          {pipelineName}
          pipelines={pipelineList.pipelines}
          onclose={() => {
            showPipelinesPanel.value = false
          }}
        ></PipelineList>
      </div>
    </Pane>
    {#if !isTablet.current}
      <PaneResizer
        class="pane-divider-vertical w-px! bg-line"
        onDraggingChange={(isDragging: boolean) => {
          isDraggingPipelineListResizer = isDragging
        }}
      ></PaneResizer>
    {/if}

    <Pane class="!overflow-visible">
      <PaneGroup
        direction="vertical"
        class="!overflow-visible"
        autoSaveId="layout/pipelines/monitoringPanel/size"
      >
        {#if pipelineBannerMessage}
          <div class="p-2 md:p-4">
            <PipelineBanner {...pipelineBannerMessage}></PipelineBanner>
          </div>
        {/if}

        <Pane defaultSize={60} minSize={15} class="!overflow-visible">
          <PaneGroup
            direction="horizontal"
            class="!overflow-visible"
            autoSaveId="layout/pipelines/interactionPanel/size"
          >
            <Pane minSize={30} class="!overflow-visible">
              {#if pipeline.current}
                <PipelineCodePanel
                  pipeline={pipeline as WritablePipeline<true>}
                  {deleted}
                  {statusBarCenter}
                  {statusBarEnd}
                  toolBarStart={pipelinesPanelToggle}
                  bind:unsavedChanges
                ></PipelineCodePanel>
              {:else}
                <div
                  class="flex h-full flex-col justify-end rounded-lg bg-surface-50-950 px-2 pt-2 pb-0"
                >
                  <div class="-mx-8 -mt-2 flex flex-1 flex-col items-center gap-4">
                    <Progress class="h-1 w-full px-4" value={null} max={100}>
                      <Progress.Track>
                        <Progress.Range class="bg-secondary-500" />
                      </Progress.Track>
                    </Progress>
                    <p class="text-surface-600-400">Loading pipeline...</p>
                  </div>
                  <div
                    class="flex flex-wrap items-center gap-x-8 border-t border-surface-200-800 px-2 py-2 pl-4"
                  >
                    <div class="flex h-6 flex-nowrap gap-3">
                      {@render statusBarCenter()}
                    </div>
                    <div class="ml-auto flex flex-nowrap gap-x-2">
                      {@render statusBarEnd()}
                    </div>
                  </div>
                </div>
              {/if}
            </Pane>
            {#if showInteractionPanel.value}
              <PaneResizer class="pane-divider-vertical w-px! bg-panel-line"></PaneResizer>
              <Pane defaultSize={40} minSize={20} class="flex flex-col !overflow-visible">
                {#if pipeline.current && metrics.current}
                  <InteractionPanel
                    pipeline={pipeline as { current: ExtendedPipeline }}
                    metrics={metrics as { current: PipelineMetrics }}
                    {deleted}
                    bind:currentTab={currentInteractionTab}
                  ></InteractionPanel>
                {:else}
                  <div class="flex flex-1 rounded-lg bg-surface-50-950 px-2 pt-3 pb-2"></div>
                {/if}
              </Pane>
            {/if}
          </PaneGroup>
        </Pane>

        {#if showMonitoringPanel.value}
          <PaneResizer class="pane-divider-horizontal h-px! bg-panel-line" />
          <Pane minSize={15} class="flex flex-col !overflow-visible">
            {#if pipeline.current && metrics.current}
              <!-- A new pipeline remounts the panel, so it picks its opening tab again -->
              {#key pipelineName}
                <MonitoringPanel
                  pipeline={pipeline as WritablePipeline<true>}
                  metrics={metrics as { current: PipelineMetrics }}
                  {deleted}
                  hiddenTabs={singleton(currentInteractionTab)}
                  bind:currentTab={currentMonitoringTab}
                  {unsavedChanges}
                ></MonitoringPanel>
              {/key}
            {:else}
              <div class="flex flex-1 rounded-lg bg-surface-50-950 px-2 pt-3 pb-2"></div>
            {/if}
          </Pane>
        {/if}
      </PaneGroup>
    </Pane>
  </PaneGroup>
</div>

{#snippet statusBarCenter()}
  {#if pipelineThumb}
    {@const programStatus = programStatusOf(pipelineThumb.status)}
    <div class="contents text-md text-surface-700-300">
      <ProgramStatus {programStatus}></ProgramStatus>
      <div class="flex items-center gap-2 pl-1">
        <PipelineVersion
          {pipelineName}
          runtimeVersion={pipelineThumb.platformVersion}
          baseRuntimeVersion={page.data.feldera!.version}
          configuredRuntimeVersion={pipelineThumb.programConfig?.runtime_version}
          {programStatus}
        ></PipelineVersion>
      </div>
    </div>
  {/if}
{/snippet}
<!-- Opens and collapses the pipelines list from its own cell at the start of the file tabs. -->
{#snippet pipelinesPanelToggle()}
  <div class="flex h-10 w-10 shrink-0 items-center justify-center border-r border-line">
    <button
      class="fd fd-panel-left btn-icon btn-icon-sm text-[16px] hover:bg-row-hover {showPipelinesPanel.value
        ? 'text-surface-950-50'
        : 'text-surface-700-300'}"
      aria-label={showPipelinesPanel.value ? 'Collapse pipelines' : 'Open pipelines'}
      aria-pressed={showPipelinesPanel.value}
      onclick={() => (showPipelinesPanel.value = !showPipelinesPanel.value)}
    ></button>
  </div>
{/snippet}
{#snippet statusBarEnd()}
  <div class="ml-auto flex flex-nowrap items-center gap-1">
    {#each layoutControls as { icon, text, value, show }}
      {#if show !== false}
        <button
          class="btn btn-sm text-surface-700-300 hover:bg-row-hover hover:text-surface-950-50"
          onclick={() => (value.value = !value.value)}
        >
          <span class="hidden text-md sm:inline">
            {text}
          </span>
          <div class="{icon} {value.value ? 'text-secondary-500' : ''}"></div>
        </button>
        <!-- A faint 16px rule between the controls. -->
        <div class="mx-1 h-4 w-px bg-surface-950-50/15" aria-hidden="true"></div>
      {/if}
    {/each}
  </div>
  <EditorOptionsPopup></EditorOptionsPopup>
{/snippet}
