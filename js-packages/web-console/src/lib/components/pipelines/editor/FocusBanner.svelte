<script lang="ts">
  import { slide } from 'svelte/transition'
  import type { Snippet } from '$lib/types/svelte'

  let {
    show,
    isFocused,
    content
  }: {
    show: boolean
    isFocused: boolean
    content: Snippet
  } = $props()

  let isInteracting = $state(false)
  let shouldShow = $derived(show && (isFocused || isInteracting))

  // Handle clicks outside to hide banner
  $effect(() => {
    if (!shouldShow) return

    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node
      const bannerElement = document.querySelector('.focus-banner')

      if (bannerElement && !bannerElement.contains(target)) {
        isInteracting = false
      }
    }

    document.addEventListener('mousedown', handleClickOutside, true)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside, true)
    }
  })
</script>

{#if shouldShow}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="focus-banner"
    transition:slide
    onmousedown={() => {
      isInteracting = true
    }}
  >
    {@render content()}
  </div>
{/if}
