<script lang="ts">
  import { slide } from 'svelte/transition'
  import type { Snippet } from '$lib/types/svelte'

  const styles = {
    // The warning tone of the status chips (see the status tones in feldera-modern.css).
    warning: {
      iconClass: 'fd fd-triangle-alert',
      colorClass: 'bg-status-warning-subtle text-status-warning'
    },
    info: {
      iconClass: 'fd fd-info',
      colorClass: 'preset-tonal-tertiary'
    }
  }
  const {
    children,
    class: classes = '',
    variant = 'warning'
  }: { children: Snippet; class?: string; variant?: keyof typeof styles } = $props()
</script>

<div transition:slide class="flex gap-2 p-2 {styles[variant].colorClass} {classes} rounded-t">
  <span class="{styles[variant].iconClass} text-[20px]"></span>
  <span>
    {@render children()}
  </span>
</div>
