<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  const {
    trigger,
    content,
    wrapperClass
  }: {
    trigger: Snippet<[toggle: () => void, isOpen: boolean]>
    content: Snippet<[close: () => void]>
    wrapperClass?: string
  } = $props()
  let show = $state(false)
  let onClose = () => {
    setTimeout(() => {
      show = false
    })
  }
  let contentNode = $state<HTMLElement>()
  const onclick = (e: MouseEvent) => {
    if (!contentNode) {
      return
    }
    if (contentNode.contains(e.target as any)) {
      return
    }
    onClose()
  }
  $effect(() => {
    if (show) {
      window.addEventListener('click', onclick, { capture: true })
    } else {
      window.removeEventListener('click', onclick)
    }
    return () => window.removeEventListener('click', onclick)
  })
</script>

<div class="relative {wrapperClass}">
  {@render trigger(() => {
    show = !show
  }, show)}
  {#if show}
    <div bind:this={contentNode}>
      {@render content(() => (show = false))}
    </div>
  {/if}
</div>
