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
    'query data'
  )
  const tabs = [
    tuple('query data', TabQueryData),
    tuple('performance', TabPerformance),
    tuple('dbsp operator graph', TabDBSPGraph),
    tuple('SQL errors', TabSQLErrors)
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
    <Tabs.Panel bind:group={currentTab.value} value={tabName}>
      <div class=" p-4 pt-0">
        <TabComponent {pipelineName}></TabComponent>
      </div>
    </Tabs.Panel>
  {/each}
{/snippet}

<Tabs list={tabList} panels={tabPanels}></Tabs>
