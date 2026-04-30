<script lang="ts" module>
  import type { Component } from 'svelte'
  import type { Snippet } from '$lib/types/svelte'

  export type TabSpec<T extends Record<string, unknown>> = {
    id: string
    label: Snippet
    panel: Component<T>
    keepAlive: boolean
    tabBarEnd?: Snippet
  }
</script>

<script lang="ts" generics="T extends Record<string, unknown>  ">
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import { SvelteSet } from 'svelte/reactivity'

  let {
    tabs,
    tabProps,
    currentTab = $bindable(),
    tabContainer = defaultTabContainer
  }: {
    tabs: TabSpec<T>[]
    tabContainer?: typeof defaultTabContainer
    tabProps: T
    currentTab: string
  } = $props()

  let visited = $state(new SvelteSet<string>())
  $effect.pre(() => {
    visited.add(currentTab)
  })
  const activeTabBarEnd = $derived(tabs.find((t) => t.id === currentTab)?.tabBarEnd)
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
  <Tabs.List class="flex w-full flex-wrap-reverse gap-0 pb-0 text-nowrap">
    {#each tabs as { id, label }}
      <Tabs.Trigger
        value={id}
        class="btn h-9 font-medium {id === currentTab
          ? 'border-surface-950-50 '
          : 'rounded hover:bg-surface-100-900/50'}"
      >
        {@render label()}
      </Tabs.Trigger>
    {/each}
    {@render activeTabBarEnd?.()}
    <Tabs.Indicator />
  </Tabs.List>
  <div class="relative h-full">
    {#each tabs as { id, panel: TabComponent, keepAlive }}
      {#snippet tab()}
        <TabComponent {...tabProps}></TabComponent>
      {/snippet}
      {#if keepAlive && visited.has(id)}
        {@render tabContainer(tab, currentTab !== id)}
      {:else if currentTab === id}
        {@render tabContainer(tab, false)}
      {/if}
    {/each}
  </div>
</Tabs>
