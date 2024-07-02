<script lang="ts">
  import type { Snippet } from 'svelte'

  let { header, content }: { header: Snippet<[boolean, () => void]>; content: Snippet } = $props()
  let open = $state(false)
</script>

{@render header(open, () => (open = !open))}
{#if open}
  {@render content()}
{/if}
