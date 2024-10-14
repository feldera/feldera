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
  import { listPipelineErrors } from '$lib/compositions/health/systemErrors.svelte'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'

  let {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()
  const pipelineName = $derived(pipeline.current.name)

  const tabs = [
    tuple('Errors' as const, TabPipelineErrors, PanelPipelineErrors),
    tuple('Performance' as const, undefined, PanelPerformance),
    tuple('Ad-hoc query' as const, TabControlAdhoc, PanelAdHocQuery),
    // tuple('query plan', TabDBSPGraph),
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
    setTimeout(() => pipelineActionCallbacks.add(pipelineName, 'start_paused', switchTo))
    return () => {
      pipelineActionCallbacks.remove(pipelineName, 'start_paused', switchTo)
    }
  })
</script>

{#snippet TabPipelineErrors(pipeline: ExtendedPipeline)}
  {@const errorCount = ((errors) => errors.programErrors.length + errors.pipelineErrors.length)(
    listPipelineErrors(pipeline)
  )}
  Errors
  {#if errorCount !== 0}
    <span class="rounded-full px-2 pt-0.5 text-sm font-medium preset-filled-error-500">
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
          classes="px-3 pt-1 rounded-none"
          labelBase=""
          translateX=""
          stateInactive="hover:bg-surface-100-900 hover:!bg-opacity-50"
          stateActive="bg-white-black"
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
    {#each tabs as [tabName, , TabComponent]}
      <Tabs.Panel value={tabName} classes="h-full overflow-y-auto relative">
        <div class="absolute h-full w-full">
          <TabComponent {pipeline} {metrics}></TabComponent>
        </div>
      </Tabs.Panel>
    {/each}
  {/snippet}
</Tabs>
