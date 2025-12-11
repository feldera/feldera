<script lang="ts">
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import type { Snippet } from '$lib/types/svelte'

  const {
    onApply,
    onClose,
    title,
    confirmLabel,
    disabled,
    children
  }: {
    onApply: () => void
    onClose: () => void
    title: Snippet
    confirmLabel: string
    disabled?: boolean
    children?: Snippet
  } = $props()
</script>

<div class="flex flex-col gap-4 p-8">
  <div class="flex flex-nowrap justify-between">
    <span class="text-2xl font-semibold">{@render title()}</span>
    <button
      onclick={onClose}
      class="preset-grayout-surface fd fd-x text-[24px]"
      aria-label="Close dialog"
    ></button>
  </div>
  {@render children?.()}
  <div class="flex w-full justify-end gap-4">
    <button onclick={onClose} class="btn preset-outlined-surface-500">Cancel</button>
    <div>
      <button {disabled} onclick={onApply} class="btn preset-filled-primary-500"
        >{confirmLabel}</button
      >
    </div>
    {#if disabled}
      <Tooltip class="z-20 w-64" placement="top">Stop the pipeline to edit settings</Tooltip>
    {/if}
  </div>
</div>
