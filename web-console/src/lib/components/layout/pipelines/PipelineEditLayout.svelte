<script lang="ts">
  import { PaneGroup, Pane, PaneResizer, type PaneAPI } from 'paneforge'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import { base } from '$app/paths'
  import {
    programStatusOf,
    type ExtendedPipeline,
    type PipelineThumb
  } from '$lib/services/pipelineManager'
  import { isPipelineCodeEditable } from '$lib/functions/pipelines/status'
  import { nonNull } from '$lib/functions/common/function'
  import ProgramStatus from '$lib/components/pipelines/editor/ProgramStatus.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import TabAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import EditorOptionsPopup from './EditorOptionsPopup.svelte'
  import { useIsTablet, useIsScreenLg } from '$lib/compositions/layout/useIsMobile.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import PipelineList from '$lib/components/pipelines/List.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import DoubleClickInput from '$lib/components/input/DoubleClickInput.svelte'
  import { goto } from '$app/navigation'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
  import PipelineVersion from '$lib/components/pipelines/editor/PipelineVersion.svelte'
  import { page } from '$app/state'
  import PipelineBanner from '$lib/components/pipelines/editor/PipelineBanner.svelte'
  import { useContextDrawer } from '$lib/compositions/layout/useContextDrawer.svelte'
  import ReviewPipelineChanges from '$lib/components/pipelines/editor/ReviewPipelineChangesDialog.svelte'
  import { parsePipelineDiff } from '$lib/functions/pipelines/pipelineDiff'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { usePipelineAction } from '$lib/compositions/usePipelineAction.svelte'
  import PipelineCodePanel from './PipelineCodePanel.svelte'
  import { Progress } from '@skeletonlabs/skeleton-svelte'

  let {
    pipelineName,
    pipelineList,
    pipelineThumb,
    pipeline
  }: {
    pipelineName: string
    pipelineList: { pipelines: PipelineThumb[] | undefined } // If it's undefined - it is loading
    pipelineThumb: PipelineThumb | undefined // If it's undefined - it is loading
    pipeline: WritablePipeline // If it's undefined - it is loading
  } = $props()

  let editCodeDisabled = $derived(
    !pipelineThumb ||
      (nonNull(pipelineThumb.status) && !isPipelineCodeEditable(pipelineThumb.status))
  )

  const pipelineAction = usePipelineAction()

  // let pipelineName = $derived(pipeline.current.name)

  const isTablet = useIsTablet()
  const drawer = useAdaptiveDrawer('right')

  let { showPipelinesPanel, showMonitoringPanel, separateAdHocTab } = useLayoutSettings()
  let isDraggingPipelineListResizer = $state(false)

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

  const contextDrawer = useContextDrawer()

  let pipelineBannerMessage = $derived.by(() => {
    if (!pipeline.current) {
      return null
    }
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

  const isScreenLg = useIsScreenLg()
  $effect(() => {
    if (!isScreenLg.current) {
      separateAdHocTab.value = false
    }
  })
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
            class="fd fd-x btn btn-icon text-[24px]"
            aria-label="Close"
            onclick={() => (contextDrawer.content = null)}
          ></button>
        {/snippet}
      </ReviewPipelineChanges>
    {/if}
  {/if}
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
              value={pipelineName}
              onvalue={(name) => {
                if (name === pipelineName) {
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
                {pipelineName}
              </span>
            </DoubleClickInput>
            {#if editCodeDisabled}
              <Tooltip class="z-10 rounded bg-white text-base text-surface-950-50 dark:bg-black">
                Cannot edit the pipeline's name while it's running
              </Tooltip>
            {/if}
          {/snippet}
        </PipelineBreadcrumbs>
        {#if pipelineThumb}
          <PipelineStatus class="h-6" status={pipelineThumb.status}></PipelineStatus>
        {/if}
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

        <Pane defaultSize={60} minSize={15} class="!overflow-visible">
          <PaneGroup direction="horizontal" class="!overflow-visible">
            <Pane minSize={30} class="!overflow-visible">
              {#if pipeline.current}
                <PipelineCodePanel
                  pipeline={pipeline as WritablePipeline<true>}
                  {editCodeDisabled}
                  {statusBarCenter}
                  {statusBarEnd}
                ></PipelineCodePanel>
              {:else}
                <div
                  class="flex h-full flex-col justify-end rounded-container px-4 py-2 bg-surface-50-950"
                >
                  <div class="-mx-8 -mt-2 flex flex-1 flex-col items-center gap-4">
                    <Progress width="w-full px-4" value={null} />
                    <p class="text-surface-600-400">Loading pipeline...</p>
                  </div>
                  <div
                    class="bg-white-dark mb-2 flex flex-wrap items-center gap-x-8 rounded-b border-t p-2 pl-4 border-surface-50-950"
                  >
                    <div class="flex h-9 flex-nowrap gap-3">
                      {@render statusBarCenter()}
                    </div>
                    <div class="ml-auto flex flex-nowrap gap-x-2">
                      {@render statusBarEnd()}
                    </div>
                  </div>
                </div>
              {/if}
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
                      {#if pipeline.current}
                        <TabAdHocQuery pipeline={pipeline as WritablePipeline<true>}
                        ></TabAdHocQuery>
                      {:else}
                        <div class="flex flex-1 rounded-container p-4 pt-3 bg-surface-50-950"></div>
                      {/if}
                    </div>
                  </div>
                </div>
              </Pane>
            {/if}
          </PaneGroup>
        </Pane>

        {#if showMonitoringPanel.value}
          <PaneResizer class="pane-divider-horizontal my-2" />
          <Pane minSize={15} class="flex flex-col !overflow-visible">
            {#if pipeline.current}
              <InteractionsPanel
                pipeline={pipeline as WritablePipeline<true>}
                separateAdHocTab={separateAdHocTab.value}
              ></InteractionsPanel>
            {:else}
              <div class="flex flex-1 rounded-container p-4 pt-3 bg-surface-50-950"></div>
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
  {/if}
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
  {#if pipeline.current}
    <!--
    In Svelte 5 (probably a bug), when rendering a Snippet (in this case, `{@render statusBarEnd()}`) in both branches of an {#if}
    the constructor of a component (in this case, <EditorOptionsPopup>) is called twice, and then one of the instances is destroyed.
    It seems like this breaks behavior of some reactive definitions, incl. useLocalStorage()
    The workaround is to avoid rendering this component on condition that matches rendering of the parent Snippet, and render a dummy instead
   -->
    <EditorOptionsPopup></EditorOptionsPopup>
  {:else}
    <button
      class="fd fd-more_horiz disabled btn btn-icon text-[20px] hover:preset-tonal-surface"
      aria-label="Editor settings"
    ></button>
  {/if}
{/snippet}
