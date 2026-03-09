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

<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import PanelAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import PanelChangeStream from '$lib/components/pipelines/editor/TabChangeStream.svelte'
  import * as TabPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import PanelPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
  import * as TabProfileVisualizer from '$lib/components/pipelines/editor/TabProfileVisualizer.svelte'
  import * as TabSamplyProfile from '$lib/components/pipelines/editor/TabSamplyProfile.svelte'
  import PanelLogs from '$lib/components/pipelines/editor/TabLogs.svelte'
  import { tuple } from '$lib/functions/common/tuple'
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
    numConnectorsWithErrors
  } from '$lib/compositions/health/systemErrors'
  import TabsPanel from './TabsPanel.svelte'

  let {
    pipeline,
    metrics,
    hiddenTabs,
    currentTab = $bindable(null)
  }: {
    pipeline: { current: ExtendedPipeline }
    metrics: { current: PipelineMetrics }
    hiddenTabs: string[]
    currentTab: MonitoringTabs | null
  } = $props()

  const pipelineName = $derived(pipeline.current.name)

  let tabs = $derived(
    [
      tuple('Errors' as const, TabControlPipelineErrors, PanelPipelineErrors, false),
      tuple(TabPerformance.id, TabControlPerformance, TabPerformance.default, false),
      tuple('Ad-Hoc Queries' as const, TabControlAdhoc, PanelAdHocQuery, false),
      tuple('Changes Stream' as const, TabControlChangeStream, PanelChangeStream, true),
      tuple(
        TabProfileVisualizer.id,
        TabProfileVisualizer.Label,
        TabProfileVisualizer.default,
        true
      ),
      tuple('Samply' as const, TabSamplyProfile.Label, TabSamplyProfile.default, false),
      tuple('Logs' as const, TabLogs, PanelLogs, false)
    ].filter((tab) => !hiddenTabs.includes(tab[0]))
  )
  const currentTabStorage = $derived(
    useLocalStorage<MonitoringTabs>('pipelines/' + pipelineName + '/currentMonitoringTab', 'Errors')
  )
  $effect.pre(() => {
    // Initialize from storage, or reset to first available tab if current is hidden
    if (!tabs.some((t) => t[0] === currentTab)) {
      currentTab = tabs.some((t) => t[0] === currentTabStorage.value)
        ? currentTabStorage.value
        : tabs[0][0]
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

  const connectorsWithErrorsCount = $derived(numConnectorsWithErrors(metrics.current))
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

<TabsPanel {tabs} bind:currentTab={currentTab!} tabProps={{ metrics, pipeline, errors }}>
  {#snippet tabBarEnd()}
    {#if currentTab !== 'Errors'}
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
    {/if}
  {/snippet}
</TabsPanel>
