<script lang="ts">
  import type { Snippet } from '#lib/types/svelte.ts'

  const {
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
      class="inline cursor-pointer self-center transition-colors {textClass} [&:not(:nth-last-child(2))]:text-surface-600-400 [&:not(:nth-last-child(2))]:hover:text-surface-950-50"
      href={breadcrumb.href}
      onclick={breadcrumb.onclick}>{breadcrumb.text}</a
    >
    <!-- The separator stays light: regular weight and the faint secondary grey. -->
    <span class="inline self-center px-2 {textClass} font-normal! text-surface-400-600! last:hidden"
      >/</span
    >
  {/each}
  {@render last?.()}
</div>
