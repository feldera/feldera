<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'

  let {
    downstreamChanged,
    saveCode,
    autoSavePipeline
  }: {
    downstreamChanged: boolean
    saveCode: () => void
    autoSavePipeline: { value: boolean }
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
  <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black">Ctrl + S</Tooltip>
  <button
    class="w-32 px-2 hover:preset-filled-primary-200-800"
    tabindex={10}
    onclick={() => (autoSavePipeline.value = !autoSavePipeline.value)}
  >
    Autosave: {autoSavePipeline.value ? 'on' : 'off'}
  </button>
</div>
