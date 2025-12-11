<script lang="ts" generics="T extends Record<string, unknown>  ">
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
  <div class="absolute flex h-full w-full flex-col sm:pt-4" class:hidden>
    {@render tab()}
  </div>
{/snippet}

<Tabs
  value={currentTab}
  onValueChange={(e) => (currentTab = e.value)}
  class="flex flex-1 flex-col space-y-0! rounded-container bg-surface-50-950 p-4"
>
  <Tabs.List class="flex w-full flex-wrap-reverse gap-0 pb-0 text-nowrap lg:flex-nowrap">
    {#each tabs as [tabName, tabControl]}
      <Tabs.Trigger
        value={tabName}
        class="btn h-9 font-medium {tabName === currentTab
          ? 'border-surface-950-50 '
          : 'rounded hover:bg-surface-100-900/50'}"
      >
        {@render tabControl()}
      </Tabs.Trigger>
    {/each}
    {@render tabBarEnd?.()}
    <Tabs.Indicator />
  </Tabs.List>
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
</Tabs>
