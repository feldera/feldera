<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import TabChangeStream from '$lib/components/pipelines/editor/TabChangeStream.svelte'
  import TabQueryData from '$lib/components/pipelines/editor/TabQueryData.svelte'
  import TabPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import TabDBSPGraph from '$lib/components/pipelines/editor/TabDBSPGraph.svelte'
  import TabPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import type { ExtendedPipeline, Pipeline } from '$lib/services/pipelineManager'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  const pipelineName = $derived(pipeline)
  let currentTab = $derived(
    useLocalStorage('pipelines/' + pipelineName + '/currentInteractionTab', 'errors')
  )
  const tabs = [
    tuple('Errors', TabPipelineErrors),
    // tuple('ad-hoc query', TabQueryData),
    tuple('Performance', TabPerformance),
    // tuple('query plan', TabDBSPGraph),
    tuple('Change Stream', TabChangeStream)
  ]
</script>

{#snippet tabList()}
  {#each tabs as [tabName]}
    <Tabs.Control
      bind:group={currentTab.value}
      name={tabName}
      contentClasses="group-hover:preset-tonal-surface"
    >
      <span>{tabName}</span>
    </Tabs.Control>
  {/each}
{/snippet}

{#snippet tabPanels()}
  {#each tabs as [tabName, TabComponent]}
    <Tabs.Panel
      bind:group={currentTab.value}
      value={tabName}
      classes="h-full overflow-y-auto relative"
    >
      <div class="ggg absolute h-full w-full p-4 pt-0">
        <TabComponent {pipeline}></TabComponent>
      </div>
    </Tabs.Panel>
  {/each}
{/snippet}

<Tabs list={tabList} panels={tabPanels} panelsClasses="flex-1" classes="flex flex-col flex-1"
></Tabs>
