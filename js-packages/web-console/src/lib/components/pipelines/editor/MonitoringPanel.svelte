<script lang="ts" module>
  export type MonitoringTabs =
    | 'Errors'
    | 'Performance'
    | 'Ad-Hoc Queries'
    | 'Changes Stream'
    | 'Profile Visualizer'
    | 'Samply'
    | 'Logs'
</script>

<!-- svelte-ignore state_referenced_locally -->
<script lang="ts">
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import PanelAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import PanelChangeStream from '$lib/components/pipelines/editor/TabChangeStream.svelte'
  import * as TabPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import PanelPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
  import * as TabProfileVisualizer from '$lib/components/pipelines/editor/TabProfileVisualizer.svelte'
  import * as TabSamplyProfile from '$lib/components/pipelines/editor/TabSamplyProfile.svelte'
  import PanelLogs from '$lib/components/pipelines/editor/TabLogs.svelte'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { count } from '$lib/functions/common/array'
  import { untrack } from 'svelte'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import DownloadSupportBundle from '$lib/components/pipelines/editor/DownloadSupportBundle.svelte'
  import {
    extractProgramErrors,
    numConnectorsWithProblems
  } from '$lib/compositions/health/systemErrors'
  import TabsPanel from './TabsPanel.svelte'

  let {
    pipeline,
    metrics,
    deleted = false,
    hiddenTabs,
    currentTab = $bindable(null)
  }: {
    pipeline: { current: ExtendedPipeline }
    metrics: { current: PipelineMetrics }
    deleted?: boolean
    hiddenTabs: string[]
    currentTab: MonitoringTabs | null
  } = $props()

  const pipelineName = $derived(pipeline.current.name)
  const layoutSettings = useLayoutSettings()

  let tabs = $derived(
    [
      {
        id: 'Errors' as const,
        label: TabControlPipelineErrors,
        panel: PanelPipelineErrors,
        keepAlive: false,
        tabBarEnd: TabBarEndCompiler
      },
      {
        id: TabPerformance.id,
        label: TabControlPerformance,
        panel: TabPerformance.default,
        keepAlive: false,
        tabBarEnd: TabBarEndPipelineInfo
      },
      {
        id: 'Ad-Hoc Queries' as const,
        label: TabControlAdhoc,
        panel: PanelAdHocQuery,
        keepAlive: false,
        tabBarEnd: TabBarEndPipelineInfo
      },
      {
        id: 'Changes Stream' as const,
        label: TabControlChangeStream,
        panel: PanelChangeStream,
        keepAlive: true,
        tabBarEnd: TabBarEndPipelineInfo
      },
      {
        id: TabProfileVisualizer.id,
        label: TabProfileVisualizer.Label,
        panel: TabProfileVisualizer.default,
        keepAlive: true,
        tabBarEnd: TabBarEndPipelineInfo
      },
      {
        id: 'Samply' as const,
        label: TabSamplyProfile.Label,
        panel: TabSamplyProfile.default,
        keepAlive: false,
        tabBarEnd: TabBarEndPipelineInfo
      },
      {
        id: 'Logs' as const,
        label: TabLogs,
        panel: PanelLogs,
        keepAlive: true,
        tabBarEnd: TabBarEndPipelineInfo
      }
    ].filter((tab) => !hiddenTabs.includes(tab.id))
  )
  const currentTabStorage = $derived(
    useLocalStorage<MonitoringTabs>('pipelines/' + pipelineName + '/currentMonitoringTab', 'Errors')
  )
  $effect.pre(() => {
    // Initialize from storage, or reset to first available tab if current is hidden
    if (!tabs.some((t) => t.id === currentTab)) {
      currentTab = tabs.some((t) => t.id === currentTabStorage.value)
        ? currentTabStorage.value
        : tabs[0].id
    }
  })
  $effect(() => {
    if (currentTab !== null) {
      currentTabStorage.value = currentTab
    }
  })

  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const forgetCurrentTab = async () => currentTabStorage.remove()
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', forgetCurrentTab))
    return () => {
      pipelineActionCallbacks.remove('', 'delete', forgetCurrentTab)
    }
  })

  let programErrors = $derived(extractProgramErrors(() => pipeline.current)(pipeline.current))
  let errors = $derived(
    programErrors.sort((a, b) =>
      a.cause.warning === b.cause.warning ? 0 : a.cause.warning ? 1 : -1
    )
  )

  const connectorsWithErrorsCount = $derived(numConnectorsWithProblems(metrics.current))

  // Updating individual properties in an $effect avoids unnecessary reactive updates within tab components
  let tabProps = $state({ metrics, pipeline, errors, deleted })
  $effect(() => {
    tabProps.metrics = metrics
  })
  $effect(() => {
    tabProps.pipeline = pipeline
  })
  $effect(() => {
    tabProps.errors = errors
  })
  $effect(() => {
    tabProps.deleted = deleted
  })
</script>

{#snippet TabControlPerformance()}
  {@render TabPerformance.Label()}
  {#if connectorsWithErrorsCount > 0}
    <span class="ml-1 inline-block min-w-6 rounded preset-filled-error-50-950 px-1 font-medium">
      {connectorsWithErrorsCount}
    </span>
  {/if}
{/snippet}

{#snippet TabControlPipelineErrors()}
  {@const warningCount = count(errors, (w) => w.cause.warning)}
  {@const errorCount = errors.length - warningCount}
  <span class="">Compiler</span>
  {#if warningCount !== 0}
    <span class="ml-1 inline-block min-w-6 rounded preset-filled-warning-200-800 px-1 font-medium">
      {warningCount}
    </span>
  {/if}
  {#if errorCount !== 0}
    <span class="ml-1 inline-block min-w-6 rounded preset-filled-error-50-950 px-1 font-medium">
      {errorCount}
    </span>
  {/if}
{/snippet}

{#snippet TabControlAdhoc()}
  <span class="inline sm:hidden"> Ad-Hoc </span>
  <span class="hidden sm:inline"> Ad-Hoc Queries </span>
{/snippet}

{#snippet TabControlChangeStream()}
  <span class="inline sm:hidden"> Changes </span>
  <span class="hidden sm:inline"> Changes Stream </span>
{/snippet}

{#snippet TabLogs()}
  <span>Logs</span>
{/snippet}

{#snippet TabBarEndPipelineInfo()}
  <div class="ml-auto flex">
    <ClipboardCopyButton
      value={pipeline.current.id}
      class="h-8 w-auto! gap-2 preset-tonal-surface px-4"
    >
      <span class="text-base font-normal text-surface-950-50"> Pipeline ID </span>
    </ClipboardCopyButton>
    <Tooltip placement="top">
      {pipeline.current.id}
    </Tooltip>
    <DownloadSupportBundle {pipelineName} />
  </div>
{/snippet}

{#snippet TabBarEndCompiler()}
  <div class="ml-auto flex flex-nowrap items-center gap-6 py-1">
    <label
      class="flex cursor-pointer items-center gap-2"
      class:disabled={layoutSettings.verbatimErrors.value}
    >
      Hide warnings
      <input class="checkbox" type="checkbox" bind:checked={layoutSettings.hideWarnings.value} />
    </label>
    <label class="flex cursor-pointer items-center gap-2 rounded">
      Verbatim errors
      <Switch
        name="verbatimErrors"
        checked={layoutSettings.verbatimErrors.value}
        onCheckedChange={(e) => (layoutSettings.verbatimErrors.value = e.checked)}
      >
        <Switch.Control>
          <Switch.Thumb />
        </Switch.Control>
        <Switch.Label />
        <Switch.HiddenInput />
      </Switch>
    </label>
  </div>
{/snippet}

<TabsPanel {tabs} bind:currentTab={currentTab!} {tabProps} />
