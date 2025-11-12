<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  let { children } = $props<{ children: Snippet }>()
</script>

{@render children()}
