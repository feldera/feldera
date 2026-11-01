<script lang="ts">
  import type { Snippet } from 'svelte'

  const {
    show,
    children,
    class: _class = ''
  }: {
    /** Whether to show `children`. */
    show: boolean
    /** Content shown next to the pointer. */
    children: Snippet
    /** Classes of the box around `children`. */
    class?: string
  } = $props()

  // The last pointer position in the window.
  let pointer = $state<{ x: number; y: number } | null>(null)
  const track = (e: PointerEvent) => (pointer = { x: e.clientX, y: e.clientY })
  // 16px right of and below the pointer, or left of and above it near the window's right and
  // bottom edges, so the box stays inside the window. The browser reports the pointer position
  // only when the pointer moves or presses; until then, the box is in the middle of the window.
  const style = $derived.by(() => {
    if (!pointer) {
      return 'left: 50%; top: 50%; transform: translate(-50%, -50%)'
    }
    const x =
      pointer.x > window.innerWidth - 320
        ? `right: ${window.innerWidth - pointer.x + 16}px`
        : `left: ${pointer.x + 16}px`
    const y =
      pointer.y > window.innerHeight - 60
        ? `bottom: ${window.innerHeight - pointer.y + 16}px`
        : `top: ${pointer.y + 16}px`
    return `${x}; ${y}`
  })
</script>

<svelte:window onpointermove={track} onpointerdown={track} />
{#if show}
  <div class="pointer-events-none fixed z-50 {_class}" {style}>
    {@render children()}
  </div>
{/if}
