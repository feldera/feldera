<script lang="ts">
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import type { Snippet } from 'svelte'
  import DoubleClickInput from '$lib/components/input/DoubleClickInput.svelte'
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import { nonNull } from '$lib/functions/common/function'

  let {
    text,
    value = $bindable(),
    close,
    tabContentChanged
  }: {
    text: Snippet
    value: string | undefined
    close: { href: string; onclick: () => void } | undefined
    tabContentChanged?: boolean
  } = $props()
</script>

<div class="relative">
  <div class="mr-5">
    {#if nonNull(value)}
      <DoubleClickInput bind:value inputClass="input -my-2 -ml-3">
        {@render text()}
      </DoubleClickInput>
    {:else}
      {@render text()}
    {/if}
  </div>
  {#if close}
    <a
      {...close}
      class={' fd fd-x btn-icon absolute right-0 top-2 h-6 text-[20px] ' +
        (tabContentChanged
          ? 'text-transparent duration-0 hover:text-inherit'
          : 'preset-grayout-surface')}
    >
      {#if tabContentChanged}
        <div
          class="gc gc-circle-solid absolute left-2.5 -m-1 w-8 p-1.5 text-xs duration-0 text-surface-950-50 hover:text-transparent"
        ></div>
      {/if}
    </a>
  {:else if tabContentChanged}
    <div class="gc gc-circle-solid absolute right-0 top-1 text-xs text-surface-600-400"></div>
  {/if}
</div>
