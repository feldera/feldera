<script lang="ts">
  import type { Snippet } from 'svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  let {
    onApply,
    onClose,
    title,
    disabled,
    children
  }: {
    onApply: () => void
    onClose: () => void
    title: Snippet
    disabled?: boolean
    children?: Snippet
  } = $props()
</script>

<div class="flex flex-col gap-4 p-4">
  <div class="flex flex-nowrap justify-between">
    {@render title()}
    <button
      onclick={onClose}
      class="preset-grayout-surface fd fd-x text-[20px]"
      aria-label="Close dialog"
    ></button>
  </div>
  {@render children?.()}
  <div class="flex w-full justify-end">
    <div>
      <button {disabled} onclick={onApply} class="btn preset-filled-primary-500"> APPLY </button>
    </div>
    {#if disabled}
      <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="top">
        Stop the pipeline to edit configuration
      </Tooltip>
    {/if}
  </div>
</div>
