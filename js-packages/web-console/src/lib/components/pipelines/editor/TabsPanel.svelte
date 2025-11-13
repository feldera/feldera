<script lang="ts" generics="T extends Record<string, unknown>">
  import type { Component } from 'svelte'
  import type { Snippet } from '$lib/types/svelte'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'

  let {
    tabs,
    tabBarEnd,
    tabProps,
    currentTab = $bindable(),
    tabContainer = defaultTabContainer
  }: {
    tabs: [string, Snippet, Component<T>, boolean][]
    tabContainer?: typeof defaultTabContainer
    tabBarEnd?: Snippet
    tabProps: T
    currentTab: string
  } = $props()
</script>

{#snippet defaultTabContainer(tab: Snippet, hidden: boolean)}
  <div class="absolute h-full w-full sm:pt-4" class:hidden>
    {@render tab()}
  </div>
{/snippet}

<Tabs
  bind:value={currentTab}
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
          {@render tabControl()}
        </Tabs.Control>
      {/each}
      {@render tabBarEnd?.()}
    </div>
  {/snippet}

  {#snippet content()}
    <div class="relative h-full">
      {#each tabs as [tabName, , TabComponent, keepAlive]}
        {#snippet tab()}
          <TabComponent {...tabProps}></TabComponent>
        {/snippet}
        {#if keepAlive}
          {@render tabContainer(tab, currentTab !== tabName)}
        {:else if currentTab === tabName}
          {@render tabContainer(tab, false)}
        {/if}
      {/each}
    </div>
  {/snippet}
</Tabs>
