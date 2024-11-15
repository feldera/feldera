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
  import {
    extractPipelineErrors,
    extractPipelineXgressErrors,
    extractProgramErrors
  } from '$lib/compositions/health/systemErrors'

  let {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()
  const pipelineName = $derived(pipeline.current.name)

  const tabs = [
    tuple('Errors' as const, TabPipelineErrors, PanelPipelineErrors),
    tuple('Performance' as const, undefined, PanelPerformance),
    tuple('Ad-hoc query' as const, TabControlAdhoc, PanelAdHocQuery),
    tuple('Change stream' as const, TabControlChangeStream, PanelChangeStream),
    tuple('Logs' as const, undefined, PanelLogs)
  ]
  let currentTab = $derived(
    useLocalStorage<(typeof tabs)[number][0]>(
      'pipelines/' + pipelineName + '/currentInteractionTab',
      'Errors'
    )
  )

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

  let programErrors = $derived(
    extractProgramErrors(() => pipeline.current)({
      name: pipeline.current.name,
      status: pipeline.current.programStatus
    })
  )
  let pipelineErrors = $derived(extractPipelineErrors(pipeline.current))
  let xgressErrors = $derived(
    extractPipelineXgressErrors({ pipelineName, status: metrics.current })
  )
  let errors = $derived([...programErrors, ...pipelineErrors, ...xgressErrors])
</script>

{#snippet TabPipelineErrors(pipeline: ExtendedPipeline)}
  {@const warningCount = count(errors, (w) => w.cause.warning)}
  {@const errorCount = errors.length - warningCount}
  <span class="pr-1">Errors</span>
  {#if warningCount !== 0}
    <span class="inline-block min-w-6 rounded-full px-1 font-medium preset-filled-warning-200-800">
      {warningCount}
    </span>
  {/if}
  {#if errorCount !== 0}
    <span class="inline-block min-w-6 rounded-full px-1 font-medium preset-filled-error-500">
      {errorCount}
    </span>
  {/if}
{/snippet}

{#snippet TabControlAdhoc()}
  <span class="inline sm:hidden"> Ad-hoc </span>
  <span class="hidden sm:inline"> Ad-hoc query </span>
{/snippet}

{#snippet TabControlChangeStream()}
  <span class="inline sm:hidden"> Changes </span>
  <span class="hidden sm:inline"> Change stream </span>
{/snippet}

<Tabs
  bind:value={currentTab.value}
  listMargin=""
  contentClasses="h-full"
  classes="flex flex-col flex-1 !space-y-0"
>
  {#snippet list()}
    <div class=" w-full">
      {#each tabs as [tabName, tabControl]}
        <Tabs.Control
          value={tabName}
          base=""
          classes="px-3 pt-1.5 h-9 rounded-none"
          labelBase=""
          translateX=""
          stateInactive="hover:bg-surface-100-900 hover:!bg-opacity-50"
          stateActive="bg-white-black outline-none"
        >
          {#if tabControl}
            {@render tabControl(pipeline.current)}
          {:else}
            <span>{tabName}</span>
          {/if}
        </Tabs.Control>
      {/each}
    </div>
  {/snippet}

  {#snippet content()}
    {@const TabComponent = tabs.find((tab) => tab[0] === currentTab.value)?.[2]}
    {#if TabComponent}
      <div class="relative h-full">
        <div class="absolute h-full w-full">
          <TabComponent {pipeline} {metrics} {errors}></TabComponent>
        </div>
      </div>
    {/if}
  {/snippet}
</Tabs>
