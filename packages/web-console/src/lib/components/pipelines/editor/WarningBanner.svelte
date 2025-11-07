<script lang="ts">
  import type { Snippet } from 'svelte'

  const styles = {
    warning: {
      iconClass: 'fd fd-triangle-alert',
      colorClass: 'preset-tonal-warning'
    },
    info: {
      iconClass: 'fd fd-info',
      colorClass: 'preset-tonal-tertiary'
    }
  }
  let {
    children,
    class: classes = '',
    variant = 'warning'
  }: { children: Snippet; class?: string; variant?: keyof typeof styles } = $props()
</script>

<div class="flex gap-2 p-2 {styles[variant].colorClass} {classes} rounded-t">
  <span class="{styles[variant].iconClass} text-[20px]"></span>
  <span>
    {@render children()}
  </span>
</div>
