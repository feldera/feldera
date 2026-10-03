<script lang="ts">
  import { clipboard, type Parameter } from '@svelte-bin/clipboard'
  import ClickFeedback from '$lib/components/common/ClickFeedback.svelte'
  import type { Snippet } from '$lib/types/svelte'

  const {
    value,
    class: _class,
    children: buttonContent
  }: { value: Parameter; class?: string; children?: Snippet } = $props()
</script>

<ClickFeedback timeoutMs={1000}>
  {#snippet children({ active, clickFeedback })}
    <!-- With a label it is a regular button with a leading 16px icon; without, a square
         icon button. -->
    <button
      class="{buttonContent
        ? 'btn before:text-[16px]'
        : 'btn-icon'} flex flex-none before:w-4 before:transition-transform {active
        ? 'fd fd-check pointer-events-none text-[20px] text-success-500 before:-translate-x-0.5'
        : 'fd fd-copy'} {_class ?? ''}"
      use:clipboard={value}
      onclick={clickFeedback}
      aria-label="Copy to clipboard"
    >
      {@render buttonContent?.()}
    </button>
  {/snippet}
</ClickFeedback>
