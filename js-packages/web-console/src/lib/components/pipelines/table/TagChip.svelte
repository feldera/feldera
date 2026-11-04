<script lang="ts" module>
  /** The look of a tag chip, for chips that other components render. */
  export const tagChipClass =
    'flex h-5 items-center gap-1.5 rounded border border-surface-200-800 px-2 text-sm whitespace-nowrap'
</script>

<script lang="ts">
  import { tagColorOf, tagDisplayName } from '#lib/functions/pipelines/tags.ts'

  let {
    tag,
    onclick,
    class: _class = '',
    ...rest
  }: {
    tag: string
    /** Makes the chip a button. Without it the chip is a static label. */
    onclick?: (e: MouseEvent) => void
    /** Add `min-w-0 max-w-full` to let the chip shrink and cut its text with an ellipsis. */
    class?: string
    'data-testid'?: string
  } = $props()
</script>

{#if onclick}
  <button class="{tagChipClass} hover:bg-surface-50-950 {_class}" {onclick} {...rest}>
    {@render content()}
  </button>
{:else}
  <span class="{tagChipClass} {_class}" {...rest}>
    {@render content()}
  </span>
{/if}

{#snippet content()}
  <span class="h-2 w-2 shrink-0 rounded-full" style="background-color: {tagColorOf(tag)}"></span>
  <span class="truncate">{tagDisplayName(tag)}</span>
{/snippet}
