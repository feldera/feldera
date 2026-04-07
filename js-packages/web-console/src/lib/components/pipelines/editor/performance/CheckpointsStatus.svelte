<script lang="ts">
  import { slide } from 'svelte/transition'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import type { CheckpointMetadata } from '$lib/services/manager'

  const {
    checkpoints,
    onClose,
    onCheckpoint
  }: {
    checkpoints: CheckpointMetadata[]
    onClose: () => void
    onCheckpoint?: () => void
  } = $props()

  let requested = $state(false)
  const handleCheckpoint = () => {
    if (!onCheckpoint) return
    requested = true
    onCheckpoint()
    // Reset after a short delay so the button becomes clickable again
    setTimeout(() => (requested = false), 3_000)
  }
</script>

<div class="bg-white-dark flex h-full flex-col gap-2 rounded p-4">
  <div class="flex items-start justify-between">
    <div class="font-medium">Checkpoints</div>
    <button class="fd fd-x text-[20px]" onclick={onClose} aria-label="Close"></button>
  </div>
  <div class="scrollbar flex-1 overflow-y-auto">
    {#if checkpoints.length === 0}
      <div class="p-2 text-surface-500">No checkpoints</div>
    {:else}
      {#each checkpoints as checkpoint}
        <InlineDropdown>
          {#snippet header(isOpen, toggle)}
            <div
              class="flex w-full cursor-pointer items-center gap-2 py-2 pr-2"
              onclick={toggle}
              role="presentation"
            >
              <div
                class="fd fd-chevron-down text-[20px] transition-transform {isOpen
                  ? 'rotate-180'
                  : ''}"
              ></div>
              <div class="flex flex-1 flex-col overflow-hidden">
                <div class="overflow-hidden text-nowrap text-ellipsis">
                  {humanSize(checkpoint.size ?? 0)} · {checkpoint.identifier ?? '—'}
                </div>
                <div class="overflow-hidden text-sm text-nowrap text-ellipsis text-surface-500">
                  {checkpoint.uuid}
                </div>
              </div>
            </div>
          {/snippet}
          {#snippet content()}
            <div class="pr-2 pb-2 pl-7" transition:slide={{ duration: 150 }}>
              <div class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
                <span class="text-surface-500">Fingerprint</span>
                <span>{checkpoint.fingerprint}</span>
                <span class="text-surface-500">Processed records</span>
                <span>{checkpoint.processed_records ?? '—'}</span>
                <span class="text-surface-500">Steps</span>
                <span>{checkpoint.steps ?? '—'}</span>
              </div>
            </div>
          {/snippet}
        </InlineDropdown>
      {/each}
    {/if}
  </div>
  {#if onCheckpoint}
    <button
      class="btn w-full preset-outlined-primary-500 btn-sm"
      onclick={handleCheckpoint}
      disabled={requested}
    >
      {requested ? 'Requesting checkpoint...' : 'Make checkpoint'}
    </button>
  {/if}
</div>
