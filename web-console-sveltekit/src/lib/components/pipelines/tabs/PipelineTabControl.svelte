<script lang="ts">
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import type { Snippet } from 'svelte'
  import DoubleClickInput from '$lib/components/input/DoubleClickInput.svelte'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import { nonNull } from '$lib/functions/common/function'

  let {
    tab,
    currentTab,
    href,
    text,
    value = $bindable(),
    close,
    tabContentChanged
  }: {
    tab: PipelineTab | 'pipelines'
    currentTab: PipelineTab
    href: string | undefined
    text: Snippet
    value: string | undefined
    close: { href: string; onclick: () => void } | undefined
    tabContentChanged?: boolean
  } = $props()
</script>

<div class="relative">
  <a {href}>
    <Tabs.Control
      group={JSON.stringify(tab)}
      name={JSON.stringify(currentTab)}
      contentClasses="group-hover:preset-tonal-surface">
      <div class="mr-4">
        {#if nonNull(value)}
          <DoubleClickInput bind:value>
            {@render text()}
          </DoubleClickInput>
        {:else}
          {@render text()}
        {/if}
      </div>
    </Tabs.Control>
  </a>
  {#if close}
    <a
      {...close}
      class={' btn-icon bx bx-x absolute right-0 top-2 h-6 text-[24px] ' +
        (tabContentChanged
          ? 'text-transparent duration-0 hover:text-inherit'
          : 'preset-grayout-surface')}>
      {#if tabContentChanged}
        <div
          class="bx bxs-circle text-surface-950-50 absolute left-2.5 -m-1 w-8 p-1.5 text-xs duration-0 hover:text-transparent">
        </div>
      {/if}
    </a>
  {:else if tabContentChanged}
    <div class="bx bxs-circle text-surface-600-400 absolute right-3 top-3 text-xs"></div>
  {/if}
</div>
