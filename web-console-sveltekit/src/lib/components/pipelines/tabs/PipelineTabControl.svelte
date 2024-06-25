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
    close
  } = $props<{
    tab: PipelineTab | 'pipelines'
    currentTab: PipelineTab
    href: string | undefined
    text: Snippet
    value: string | undefined
    close: { href: string; onclick: () => void } | undefined
  }>()
</script>

<div class="relative">
  <a {href}>
    <Tabs.Control
      group={JSON.stringify(tab)}
      name={JSON.stringify(currentTab)}
      contentClasses="group-hover:preset-tonal-surface"
    >
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
      class="preset-grayout-surface bx bx-x btn-icon absolute right-0 top-2 h-6 text-[24px]"
    >
    </a>
  {/if}
</div>
