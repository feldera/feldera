<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import TabQueryData from '$lib/components/pipelines/editor/TabQueryData.svelte'
  import TabPerformance from '$lib/components/pipelines/editor/TabPerformance.svelte'
  import TabDBSPGraph from '$lib/components/pipelines/editor/TabDBSPGraph.svelte'
  import TabSQLErrors from '$lib/components/pipelines/editor/TabSQLErrors.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  let { pipelineName }: { pipelineName: string } = $props()
  let currentTab = useLocalStorage(
    'pipelines/' + pipelineName + '/currentInteractionTab',
    'performance'
  )
  const tabs = [
    tuple('SQL errors', TabSQLErrors),
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
    <Tabs.Panel bind:group={currentTab.value} value={tabName} classes="grow p-4 pt-0">
      <TabComponent {pipelineName}></TabComponent>
    </Tabs.Panel>
  {/each}
{/snippet}

<Tabs list={tabList} panels={tabPanels} classes="h-full"></Tabs>
