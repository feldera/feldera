<script lang="ts">
  import { scale } from 'svelte/transition'
  import type { StickToBottom } from './stickToBottom.svelte'

  const {
    stickToBottom,
    class: className = ''
  }: {
    // Reads the raw `stuck` value, not a debounced one: the button has to disappear on the press
    // that re-arms the stick, or the user reads the delay as a press that did not register.
    stickToBottom: Pick<StickToBottom, 'stick' | 'stuck'>
    class?: string
  } = $props()
</script>

{#if !stickToBottom.stuck}
  <button
    transition:scale={{ duration: 200 }}
    class="fd fd-arrow-down absolute right-4 bottom-4 z-20 rounded-full preset-filled-primary-500 p-2 text-[20px] {className}"
    onclick={() => stickToBottom.stick()}
    aria-label="Scroll to bottom"
  ></button>
{/if}
