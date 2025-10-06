<script lang="ts">
  import type { Snippet } from 'svelte'

  let { dialog, onClose }: { dialog: Snippet | null; onClose: () => void } = $props()

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
    window.addEventListener('click', onclick, { capture: true })
    return () => window.removeEventListener('click', onclick)
  })
</script>

{#if dialog}
  <div class="relative z-40" aria-labelledby="modal-title" role="dialog" aria-modal="true">
    <!--
    Background backdrop, show/hide based on modal state.

    Entering: "ease-out duration-300"
      From: "opacity-0"
      To: "opacity-100"
    Leaving: "ease-in duration-200"
      From: "opacity-100"
      To: "opacity-0"
  -->
    <div
      class="fixed inset-0 cursor-pointer bg-gray-500 bg-opacity-75 transition-opacity"
      role="presentation"
      aria-hidden="true"
    ></div>

    <div class="fixed inset-0 w-screen overflow-y-auto">
      <div
        class="flex min-h-full items-end justify-center p-4 text-center sm:items-center sm:p-0"
        role="presentation"
        onclick={onClose}
      >
        <!--
        Modal panel, show/hide based on modal state.

        Entering: "ease-out duration-300"
          From: "opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
          To: "opacity-100 translate-y-0 sm:scale-100"
        Leaving: "ease-in duration-200"
          From: "opacity-100 translate-y-0 sm:scale-100"
          To: "opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
      -->
        <div
          class="relative w-full transform rounded-container bg-white text-left shadow-xl transition-all scrollbar sm:my-8 sm:max-w-2xl dark:bg-dark"
          role="presentation"
          onclick={(e) => {
            e.stopPropagation()
          }}
          bind:this={contentNode}
        >
          {@render dialog()}
        </div>
      </div>
    </div>
  </div>
{/if}
