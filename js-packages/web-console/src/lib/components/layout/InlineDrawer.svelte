<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  const {
    side,
    open,
    children,
    width
  }: {
    open: boolean
    side: 'right' | 'left'
    children: Snippet
    width: string
  } = $props()

  const positionClass = {
    right: 'left-0',
    left: 'right-0 '
  }

  const shadowClass = {
    right: 'shadow-[-1px_0px_4px_0px_rgba(0,0,0,0.1)] [clip-path:inset(0_0_0_-5px)] pl-4 ml-4',
    left: 'shadow-[1px_0px_-4px_0px_rgba(0,0,0,0.1)] [clip-path:inset(0_5px_0_0)] pr-4 mr-4'
  }
</script>

<div class="{open ? shadowClass[side] : ''} overflow-clip">
  <div
    class={' relative h-full transition-[width] duration-300 ease-in-out ' +
      (open ? width : 'w-0 ')}
  >
    <div class=" absolute h-full {width} {positionClass[side]}">
      {@render children()}
    </div>
  </div>
</div>
