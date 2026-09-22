<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  let {
    trigger,
    content,
    wrapperClass,
    isOpen = $bindable(false)
  }: {
    trigger: Snippet<[toggle: () => void, isOpen: boolean]>
    content: Snippet<[close: () => void]>
    wrapperClass?: string
    /**
     * Whether the content is shown. Bind to it to open or close the popup from
     * somewhere other than the trigger's own click handler.
     * A click outside the popup closes it whether or not anything is bound here.
     */
    isOpen?: boolean
  } = $props()
  const onClose = () => {
    setTimeout(() => {
      isOpen = false
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
    if (isOpen) {
      window.addEventListener('click', onclick, { capture: true })
    } else {
      window.removeEventListener('click', onclick)
    }
    return () => window.removeEventListener('click', onclick)
  })
</script>

<div class="relative {wrapperClass}">
  {@render trigger(() => {
    isOpen = !isOpen
  }, isOpen)}
  {#if isOpen}
    <div bind:this={contentNode}>
      {@render content(() => (isOpen = false))}
    </div>
  {/if}
</div>
