<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'

  let { downstreamChanged, saveCode }: { downstreamChanged: boolean; saveCode: () => void } =
    $props()

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)
</script>

<div class="flex h-full flex-nowrap">
  <!-- <div class="w-20 p-0.5 text-center">
    {downstreamChanged ? 'changed' : 'saved'}
  </div> -->
  <button
    class={'px-4 hover:preset-filled-primary-200-800 ' +
      (downstreamChanged ? 'preset-tonal-primary' : 'bg-surface-50-950')}
    class:disabled={!downstreamChanged}
    onclick={saveCode}
  >
    SAVE
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
