<script lang="ts">
  import type { Snippet } from 'svelte'

  let { content }: { content: Snippet } = $props()
</script>

{@render content()}
