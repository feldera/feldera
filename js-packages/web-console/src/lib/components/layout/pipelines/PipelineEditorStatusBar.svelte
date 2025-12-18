<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'

  const {
    downstreamChanged,
    saveCode,
    autoSaveFiles
  }: {
    downstreamChanged: boolean
    saveCode: () => void
    autoSaveFiles: { value: boolean }
  } = $props()
</script>

<div class="flex h-full flex-nowrap">
  <button
    class={'w-20 px-4 hover:preset-filled-primary-200-800 ' +
      (downstreamChanged ? 'preset-tonal-primary' : 'bg-surface-50-950')}
    class:disabled={!downstreamChanged}
    onclick={saveCode}
  >
    {downstreamChanged ? 'Save' : 'Saved'}
  </button>
  <Tooltip class="">Ctrl + S</Tooltip>
  <button
    class="w-32 px-2 hover:preset-filled-primary-200-800"
    tabindex={10}
    onclick={() => (autoSaveFiles.value = !autoSaveFiles.value)}
  >
    Autosave: {autoSaveFiles.value ? 'on' : 'off'}
  </button>
</div>
