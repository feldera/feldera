<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import PanelChangeStream from '$lib/components/pipelines/editor/TabChangeStream.svelte'
  import TabQueryData from '$lib/components/pipelines/editor/TabQueryData.svelte'
  import PanelPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import TabDBSPGraph from '$lib/components/pipelines/editor/TabDBSPGraph.svelte'
  import PanelPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import type { ExtendedPipeline, Pipeline } from '$lib/services/pipelineManager'
  import { listPipelineErrors } from '$lib/compositions/health/systemErrors.svelte'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  let {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()
  const pipelineName = $derived(pipeline)
  let currentTab = $derived(
    useLocalStorage('pipelines/' + pipelineName + '/currentInteractionTab', 'errors')
  )
  const tabs = [
    tuple('Errors', TabPipelineErrors, PanelPipelineErrors),
    // tuple('ad-hoc query', TabQueryData),
    tuple('Performance', undefined, PanelPerformance),
    // tuple('query plan', TabDBSPGraph),
    tuple('Changes stream', undefined, PanelChangeStream)
  ]
</script>

{#snippet TabPipelineErrors(pipeline: ExtendedPipeline)}
  {@const errorCount = ((errors) => errors.programErrors.length + errors.pipelineErrors.length)(
    listPipelineErrors(pipeline)
  )}
  Errors
  {#if errorCount !== 0}
    <span class="preset-filled-error-500 rounded-full px-2 pt-0.5 text-sm font-medium">
      {errorCount}
    </span>
  {/if}
{/snippet}

{#snippet tabList()}
  {#each tabs as [tabName, tabControl]}
    <Tabs.Control
      bind:group={currentTab.value}
      name={tabName}
      contentClasses="group-hover:preset-tonal-surface">
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
      classes="h-full overflow-y-auto relative">
      <div class="ggg absolute h-full w-full p-4 pt-0">
        <TabComponent {pipeline} {metrics}></TabComponent>
      </div>
    </Tabs.Panel>
  {/each}
{/snippet}

<Tabs list={tabList} panels={tabPanels} panelsClasses="flex-1" classes="flex flex-col flex-1"
></Tabs>
