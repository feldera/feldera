<script lang="ts">
  import { scale } from 'svelte/transition'

  const {
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
    class="fd fd-arrow-down absolute right-4 bottom-4 z-20 rounded-full preset-filled-primary-500 p-2 text-[20px]"
    onclick={() => {
      reverseScroll.stickToBottom = true
      reverseScroll.scrollToBottom()
    }}
    aria-label="Scroll to bottom"
  ></button>
{/if}
