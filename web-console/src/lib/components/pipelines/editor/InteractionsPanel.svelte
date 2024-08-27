<script lang="ts" context="module">
  const pipelineActionCallbacks = usePipelineActionCallbacks()
</script>

<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import PanelChangeStream from '$lib/components/pipelines/editor/TabChangeStream.svelte'
  import PanelPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import PanelPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
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
    // tuple('ad-hoc query', TabQueryData),
    tuple('Performance' as const, undefined, PanelPerformance),
    // tuple('query plan', TabDBSPGraph),
    tuple('Changes stream' as const, undefined, PanelChangeStream)
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

{#snippet tabList()}
  {#each tabs as [tabName, tabControl]}
    <Tabs.Control
      bind:group={currentTab.value}
      name={tabName}
      contentClasses="group-hover:preset-tonal-surface"
    >
      {#if tabControl}
        {@render tabControl(pipeline.current)}
      {:else}
        <span>{tabName}</span>
      {/if}
    </Tabs.Control>
  {/each}
{/snippet}

{#snippet tabPanels()}
  {#each tabs as [tabName,, TabComponent]}
    <Tabs.Panel
      bind:group={currentTab.value}
      value={tabName}
      classes="h-full overflow-y-auto relative"
    >
      <div class="absolute h-full w-full">
        <TabComponent {pipeline} {metrics}></TabComponent>
      </div>
    </Tabs.Panel>
  {/each}
{/snippet}

<Tabs
  list={tabList}
  panels={tabPanels}
  panelsClasses="flex-1"
  classes="flex flex-col flex-1 !space-y-0"
></Tabs>
