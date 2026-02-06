<script lang="ts" module>
  const pipelineActionCallbacks = usePipelineActionCallbacks()
</script>

<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import PanelAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import PanelChangeStream from '$lib/components/pipelines/editor/TabChangeStream.svelte'
  import PanelPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import PanelPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
  import * as TabProfileVisualizer from '$lib/components/pipelines/editor/TabProfileVisualizer.svelte'
  import * as TabSamplyProfile from '$lib/components/pipelines/editor/TabSamplyProfile.svelte'
  import * as TabPipelineEvents from '$lib/components/pipelines/editor/TabPipelineEvents.svelte'
  import PanelLogs from '$lib/components/pipelines/editor/TabLogs.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { count } from '$lib/functions/common/array'
  import { untrack } from 'svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import DownloadSupportBundle from '$lib/components/pipelines/editor/DownloadSupportBundle.svelte'
  import {
    extractPipelineErrors,
    extractPipelineXgressErrors,
    extractProgramErrors
  } from '$lib/compositions/health/systemErrors'
  import TabsPanel from './TabsPanel.svelte'

  let {
    pipeline,
    metrics,
    currentInteractionTab
  }: {
    pipeline: { current: ExtendedPipeline }
    metrics: { current: PipelineMetrics }
    currentInteractionTab: string | null
  } = $props()

  const pipelineName = $derived(pipeline.current.name)

  let tabs = $derived(
    [
      tuple('Errors' as const, TabControlPipelineErrors, PanelPipelineErrors, false),
      tuple('Performance' as const, TabControlPerformance, PanelPerformance, false),
      tuple('Ad-Hoc Queries' as const, TabControlAdhoc, PanelAdHocQuery, false),
      tuple('Changes Stream' as const, TabControlChangeStream, PanelChangeStream, true),
      tuple(
        'Profile Visualizer' as const,
        TabProfileVisualizer.Label,
        TabProfileVisualizer.default,
        true
      ),
      tuple('Samply' as const, TabSamplyProfile.Label, TabSamplyProfile.default, false),
      tuple('Logs' as const, TabLogs, PanelLogs, false),
      tuple('Pipeline Health' as const, TabPipelineEvents.Label, TabPipelineEvents.default, true)
    ].filter((tab) => tab[0] !== currentInteractionTab)
  )
  let currentTab = $derived(
    useLocalStorage<(typeof tabs)[number][0]>(
      'pipelines/' + pipelineName + '/currentMonitoringTab',
      'Errors'
    )
  )
  $effect.pre(() => {
    // Switch to the first available tab if the current tab was opened in another panel
    if (!tabs.some((tab) => tab[0] === currentTab.value)) currentTab.value = tabs[0][0]
  })

  const switchTo = async () => {
    if (currentTab.value === 'Errors') {
      currentTab.value = 'Performance'
    }
  }
  $effect(() => {
    pipelineName
    untrack(() => pipelineActionCallbacks.add(pipelineName, 'start', switchTo))
    return () => {
      pipelineActionCallbacks.remove(pipelineName, 'start', switchTo)
    }
  })
  const forgetCurrentTab = async () => currentTab.remove()
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', forgetCurrentTab))
    return () => {
      pipelineActionCallbacks.remove('', 'delete', forgetCurrentTab)
    }
  })

  let programErrors = $derived(extractProgramErrors(() => pipeline.current)(pipeline.current))
  let pipelineErrors = $derived(extractPipelineErrors(pipeline.current))
  let xgressErrors = $derived(
    extractPipelineXgressErrors({ pipelineName, status: metrics.current })
  )
  let errors = $derived(
    [...programErrors, ...pipelineErrors, ...xgressErrors].sort((a, b) =>
      a.cause.warning === b.cause.warning ? 0 : a.cause.warning ? 1 : -1
    )
  )
</script>

{#snippet TabControlPipelineErrors()}
  {@const warningCount = count(errors, (w) => w.cause.warning)}
  {@const errorCount = errors.length - warningCount}
  <span class="pr-1">Errors</span>
  {#if warningCount !== 0}
    <span class="inline-block min-w-6 rounded preset-filled-warning-200-800 px-1 font-medium">
      {warningCount}
    </span>
  {/if}
  {#if errorCount !== 0}
    <span class="inline-block min-w-6 rounded preset-filled-error-50-950 px-1 font-medium">
      {errorCount}
    </span>
  {/if}
{/snippet}

{#snippet TabControlPerformance()}
  <span class="inline sm:hidden"> Perf </span>
  <span class="hidden sm:inline"> Performance </span>
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

<TabsPanel {tabs} bind:currentTab={currentTab.value} tabProps={{ metrics, pipeline, errors }}>
  {#snippet tabBarEnd()}
    {#if currentTab.value !== 'Errors'}
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
