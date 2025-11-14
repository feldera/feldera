<script lang="ts">
  import { scale } from 'svelte/transition'

  let {
    reverseScroll
  }: {
    reverseScroll: {
      scrollToBottom: (lastOffset?: number) => void
      stickToBottom: boolean
    }
  } = $props()
</script>

{#if !reverseScroll.stickToBottom}
  <button
    transition:scale={{ duration: 200 }}
    class="fd fd-arrow-down absolute bottom-4 right-4 z-20 rounded-full p-2 text-[20px] preset-filled-primary-500"
    onclick={() => {
      reverseScroll.stickToBottom = true
      reverseScroll.scrollToBottom()
    }}
    aria-label="Scroll to bottom"
  ></button>
{/if}
