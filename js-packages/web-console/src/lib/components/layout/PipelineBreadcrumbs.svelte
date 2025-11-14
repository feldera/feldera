<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  let {
    breadcrumbs,
    last,
    class: _class,
    textClass
  }: {
    breadcrumbs: { text: string; href?: string; onclick?: () => void }[]
    last?: Snippet
    class?: string
    textClass?: string
  } = $props()
</script>

<div class="flex flex-nowrap overflow-hidden overflow-ellipsis whitespace-nowrap {_class}">
  {#each breadcrumbs as breadcrumb}
    <a
      class="inline cursor-pointer self-center {textClass} [&:not(:nth-last-child(2))]:text-surface-500"
      href={breadcrumb.href}
      onclick={breadcrumb.onclick}>{breadcrumb.text}</a
    >
    <span class="inline self-center px-2 {textClass} !text-surface-500 last:hidden">/</span>
  {/each}
  {@render last?.()}
</div>
