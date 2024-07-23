<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import TabQueryData from '$lib/components/pipelines/editor/TabQueryData.svelte'
  import TabPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import TabDBSPGraph from '$lib/components/pipelines/editor/TabDBSPGraph.svelte'
  import TabPipelineErrors from '$lib/components/pipelines/editor/TabPipelineErrors.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  let { pipelineName }: { pipelineName: string } = $props()
  let currentTab = useLocalStorage(
    'pipelines/' + pipelineName + '/currentInteractionTab',
    'performance'
  )
  const tabs = [
    tuple('errors', TabPipelineErrors),
    tuple('ad-hoc query', TabQueryData),
    tuple('performance', TabPerformance),
    tuple('query plan', TabDBSPGraph)
  ]
</script>

{#snippet tabList()}
  {#each tabs as [tabName]}
    <Tabs.Control
      bind:group={currentTab.value}
      name={tabName}
      contentClasses="group-hover:preset-tonal-surface">
      <span>{tabName}</span>
    </Tabs.Control>
  {/each}
{/snippet}

{#snippet tabPanels()}
  {#each tabs as [tabName, TabComponent]}
    <Tabs.Panel
      bind:group={currentTab.value}
      value={tabName}
      classes="h-full overflow-y-auto relative">
      <div class="absolute h-full w-full p-4 pt-0">
        <TabComponent {pipelineName}></TabComponent>
      </div>
    </Tabs.Panel>
  {/each}
{/snippet}

<Tabs list={tabList} panels={tabPanels} classes="flex flex-col flex-1"></Tabs>
