<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  let {
    trigger,
    content,
    wrapperClass,
    open: show = $bindable(false)
  }: {
    trigger: Snippet<[toggle: () => void, isOpen: boolean]>
    content: Snippet<[close: () => void]>
    wrapperClass?: string
    /**
     * Whether the content is shown. Bind to it to open or close the popup from
     * somewhere other than the trigger's own click. A click outside the popup closes
     * it whether or not anything is bound here.
     */
    open?: boolean
  } = $props()
  const onClose = () => {
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
