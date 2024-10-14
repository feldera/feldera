<script lang="ts">
  import type { Snippet } from 'svelte'

  let { children, class: classes = '' }: { children: Snippet; class?: string } = $props()
</script>

<div class="flex gap-1 p-1 preset-tonal-warning {classes}">
  <span class="fd fd-warning_amber text-[24px]"></span>
  <span>
    {@render children()}
  </span>
</div>
