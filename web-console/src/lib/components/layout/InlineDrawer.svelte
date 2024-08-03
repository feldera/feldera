<script lang="ts">
  import type { Snippet } from 'svelte'
  let {
    open,
    children,
    width
  }: {
    open: boolean
    side: 'right' | 'left' | 'top' | 'bottom'
    children: Snippet
    width: string
  } = $props()
</script>

<div class={''}>
  <div class={'relative transition-[width] duration-300 ease-in-out ' + (open ? width : 'w-0 ')}>
    <div class={'absolute right-0 ' + width}>
      {@render children()}
    </div>
  </div>
</div>
