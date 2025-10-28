<script lang="ts" module>
  const pipelineActionCallbacks = usePipelineActionCallbacks()
</script>

<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import PanelAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import PanelChangeStream from '$lib/components/pipelines/editor/TabChangeStream.svelte'
  import PanelPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import PanelPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
  import PanelLogs from '$lib/components/pipelines/editor/TabLogs.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { count } from '$lib/functions/common/array'
  import { untrack } from 'svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import DownloadSupportBundle from '$lib/components/pipelines/editor/DownloadSupportBundle.svelte'
  import { useAggregatePipelineStats } from '$lib/compositions/useAggregatePipelineStats.svelte'
  import {
    extractPipelineErrors,
    extractPipelineXgressErrors,
    extractProgramErrors
  } from '$lib/compositions/health/systemErrors'
  import { nonNull } from '$lib/functions/common/function'

  let {
    pipeline,
    separateAdHocTab
  }: {
    pipeline: { current: ExtendedPipeline }
    separateAdHocTab: boolean
  } = $props()
  const pipelineName = $derived(pipeline.current.name)

  let metrics = useAggregatePipelineStats(pipeline, 2000, 63000)

  let tabs = $derived(
    [
      tuple('Errors' as const, TabControlPipelineErrors, PanelPipelineErrors),
      tuple('Performance' as const, TabControlPerformance, PanelPerformance),
      separateAdHocTab ? null : tuple('Ad-Hoc Queries' as const, TabControlAdhoc, PanelAdHocQuery),
      tuple('Changes Stream' as const, TabControlChangeStream, PanelChangeStream),
      tuple('Logs' as const, undefined, PanelLogs)
    ].filter(nonNull)
  )
  let currentTab = $derived(
    useLocalStorage<(typeof tabs)[number][0]>(
      'pipelines/' + pipelineName + '/currentInteractionTab',
      'Errors'
    )
  )
  $effect.pre(() => {
    if (separateAdHocTab && currentTab.value === 'Ad-Hoc Queries') {
      currentTab.value = 'Errors'
    }
  })

  const switchTo = async () => {
    if (currentTab.value === 'Errors') {
      currentTab.value = 'Performance'
    }
  }
  $effect(() => {
    pipelineName
    untrack(() => pipelineActionCallbacks.add(pipelineName, 'start_paused', switchTo))
    return () => {
      pipelineActionCallbacks.remove(pipelineName, 'start_paused', switchTo)
    }
  })
  const forgetCurrentTab = async () => currentTab.remove()
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', forgetCurrentTab))
    return () => {
      pipelineActionCallbacks.remove('', 'start_paused', forgetCurrentTab)
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

{#snippet TabControlPipelineErrors(pipeline: ExtendedPipeline)}
  {@const warningCount = count(errors, (w) => w.cause.warning)}
  {@const errorCount = errors.length - warningCount}
  <span class="pr-1">Errors</span>
  {#if warningCount !== 0}
    <span class="inline-block min-w-6 rounded px-1 font-medium preset-filled-warning-200-800">
      {warningCount}
    </span>
  {/if}
  {#if errorCount !== 0}
    <span class="inline-block min-w-6 rounded px-1 font-medium preset-filled-error-50-950">
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

<Tabs
  bind:value={currentTab.value}
  listMargin=""
  contentClasses="h-full"
  classes="flex flex-col flex-1 !space-y-0 bg-surface-50-950 rounded-container p-4 pt-3"
>
  {#snippet list()}
    <div class="flex w-full flex-wrap text-nowrap lg:flex-nowrap">
      {#each tabs as [tabName, tabControl]}
        <Tabs.Control
          value={tabName}
          base=""
          classes="px-3 py-2 font-medium"
          labelBase=""
          translateX=""
          stateInactive="rounded hover:!bg-opacity-50 hover:bg-surface-100-900"
          stateActive="inset-y-2 border-b-2 pb-1.5 border-surface-950-50 outline-none"
        >
          {#if tabControl}
            {@render tabControl(pipeline.current)}
          {:else}
            <span>{tabName}</span>
          {/if}
        </Tabs.Control>
      {/each}
      {#if currentTab.value !== 'Errors'}
        <div class="ml-auto flex">
          <ClipboardCopyButton value={pipeline.current.id} class="h-8 w-auto preset-tonal-surface">
            <span class="text-base font-normal text-surface-950-50"> Pipeline ID </span>
          </ClipboardCopyButton>
          <Tooltip
            placement="top"
            class="z-10 text-nowrap rounded bg-white text-base text-surface-950-50 dark:bg-black"
          >
            {pipeline.current.id}
          </Tooltip>
          <DownloadSupportBundle {pipelineName} />
        </div>
      {/if}
    </div>
  {/snippet}

  {#snippet content()}
    {@const TabComponent = tabs.find((tab) => tab[0] === currentTab.value)?.[2]}
    {#if TabComponent}
      <div class="relative h-full">
        <div class="absolute h-full w-full sm:pt-4">
          <TabComponent {pipeline} {metrics} {errors}></TabComponent>
        </div>
      </div>
    {/if}
  {/snippet}
</Tabs>
