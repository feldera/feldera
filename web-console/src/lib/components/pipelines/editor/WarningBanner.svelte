<script lang="ts">
  import type { Snippet } from 'svelte'

  let { children, class: classes = '' }: { children: Snippet; class?: string } = $props()
</script>

<div class="flex gap-2 p-2 preset-tonal-warning {classes}">
  <span class="fd fd-triangle-alert text-[20px]"></span>
  <span>
    {@render children()}
  </span>
</div>
