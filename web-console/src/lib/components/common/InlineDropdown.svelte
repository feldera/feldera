<script lang="ts">
  import type { Snippet } from 'svelte'

  let {
    header,
    content,
    startOpen
  }: { header: Snippet<[boolean, () => void]>; content: Snippet; startOpen?: boolean } = $props()
  let open = $state(!!startOpen)
</script>

{@render header(open, () => (open = !open))}
{#if open}
  {@render content()}
{/if}
