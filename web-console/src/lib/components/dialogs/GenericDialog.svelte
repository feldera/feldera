<script lang="ts">
  import type { Snippet } from 'svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  let {
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
      class="preset-grayout-surface fd fd-x text-[20px]"
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
      <Tooltip class="bg-white-dark z-20 w-64 rounded text-surface-950-50" placement="top">
        Stop the pipeline to edit settings
      </Tooltip>
    {/if}
  </div>
</div>
