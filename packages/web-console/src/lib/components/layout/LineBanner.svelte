<script lang="ts" module>
  export { BannerButton }
</script>

<script lang="ts">
  import type { Snippet } from 'svelte'

  const variants = {
    formal: 'bg-primary-900 dark:bg-primary-600 text-surface-50',
    aether: 'bg-gradient-to-r from-orange-100 to-purple-100 dark:from-orange-800 dark:to-purple-900'
  }

  const {
    start,
    center,
    end,
    variant = 'formal',
    dismiss
  }: {
    start?: Snippet
    center?: Snippet
    end?: Snippet
    variant?: keyof typeof variants
    dismiss?: () => void
  } = $props()
</script>

{#snippet BannerButton({
  text,
  ariaLabel,
  href,
  onclick,
  class: _class
}: {
  text?: string
  ariaLabel?: string
  href?: string
  onclick?: () => void
  class?: string
})}
  {#if onclick}
    <button
      {onclick}
      class="rounded bg-surface-50 px-2 py-1 text-sm text-dark hover:brightness-90 {_class}"
      aria-label={ariaLabel}
    >
      {text}
    </button>
  {:else}
    <a
      {href}
      target="_blank"
      rel="noreferrer"
      class="rounded bg-surface-50 px-2 py-1 text-sm text-dark hover:brightness-90 {_class}"
    >
      {text}
    </a>
  {/if}
{/snippet}

<div
  class="{variants[
    variant
  ]} flex min-h-12 flex-nowrap items-center justify-between gap-1 px-2 py-2 text-base font-medium sm:px-8"
>
  <div class="flex flex-nowrap items-center gap-2 sm:gap-6">
    {@render start?.()}
  </div>
  <div class="flex flex-nowrap items-center gap-2 sm:gap-6">
    {@render center?.()}
  </div>
  <div class="flex flex-nowrap items-center gap-2 sm:gap-6">
    {@render end?.()}
    {#if dismiss}
      <button
        onclick={dismiss}
        aria-label="Dismiss message"
        class="fd fd-x px-1 text-2xl hover:brightness-90"
      ></button>
    {/if}
  </div>
</div>
