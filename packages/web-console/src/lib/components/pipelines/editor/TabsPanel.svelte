<script lang="ts" generics="T extends Record<string, unknown>">
  import type { Component } from 'svelte'
  import type { Snippet } from '$lib/types/svelte'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'

  let {
    tabs,
    tabBarEnd,
    tabProps,
    currentTab = $bindable()
  }: {
    tabs: [string, Snippet, Component<T>][]
    tabBarEnd?: Snippet
    tabProps: T
    currentTab: string
  } = $props()
</script>

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
    {@const TabComponent = tabs.find((tab) => tab[0] === currentTab)?.[2]}
    {#if TabComponent}
      <div class="relative h-full">
        <div class="absolute h-full w-full sm:pt-4">
          <TabComponent {...tabProps}></TabComponent>
        </div>
      </div>
    {/if}
  {/snippet}
</Tabs>
