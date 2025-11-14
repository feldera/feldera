<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  let {
    header,
    content,
    startOpen,
    open = $bindable(startOpen ?? false)
  }: {
    header: Snippet<[boolean, () => void]>
    content: Snippet
    open?: boolean
    startOpen?: boolean
  } = $props()
</script>

{@render header(open, () => (open = !open))}
{#if open}
  {@render content()}
{/if}
