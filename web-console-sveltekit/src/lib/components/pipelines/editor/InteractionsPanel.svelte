<script lang="ts">
  import { localStore } from '$lib/compositions/localStore.svelte'
  import TabQueryData from '$lib/components/pipelines/editor/TabQueryData.svelte'
  import TabPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import TabDBSPGraph from '$lib/components/pipelines/editor/TabDBSPGraph.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  let { pipelineName } = $props<{ pipelineName: string }>()
  let currentTab = localStore('pipelines/' + pipelineName + '/currentInteractionTab', 'query data')
  const tabs = [
    tuple('query data', TabQueryData),
    tuple('performance', TabPerformance),
    tuple('dbsp operator graph', TabDBSPGraph)
  ]
</script>

{#snippet tabList()}
  {#each tabs as [tabName, TabComponent]}
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
    <Tabs.Panel bind:group={currentTab.value} value={tabName}
      ><TabComponent></TabComponent></Tabs.Panel
    >
  {/each}
{/snippet}

<Tabs list={tabList} panels={tabPanels}></Tabs>
