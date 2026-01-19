<script lang="ts">
  import { useToast } from '$lib/compositions/useToastNotification'
  import { displaySQLValue, serializeSQLValue } from '$lib/functions/sql'
  import type { SQLValueJS } from '$lib/types/sql'
  import ClipboardCopyButton from './ClipboardCopyButton.svelte'

  let {
    popupRef = $bindable(),
    tooltipData
  }: {
    popupRef?: HTMLElement
    tooltipData?: { x: number; y: number; targetWidth: number; value: SQLValueJS }
  } = $props()

  const toast = useToast()
</script>

<div
  class="bg-white-dark absolute m-0 w-max max-w-lg -translate-x-[4.5px] -translate-y-[2.5px] border border-surface-500 px-2 py-1 break-words whitespace-break-spaces text-surface-950-50"
  popover="manual"
  bind:this={popupRef}
  style={tooltipData
    ? `left: ${tooltipData.x}px; top: ${tooltipData.y}px; min-width: ${tooltipData.targetWidth + 8}px`
    : ''}
>
  <div class="flex flex-nowrap justify-between gap-2">
    <span class="flex-1">
      {(tooltipData && toast.catchError(displaySQLValue)(tooltipData.value)) || ''}
    </span>
    {#if tooltipData}
      <ClipboardCopyButton
        class="flex-none p-0"
        value={() => toast.catchError(serializeSQLValue)(tooltipData.value) ?? ''}
      ></ClipboardCopyButton>
    {/if}
  </div>
</div>
