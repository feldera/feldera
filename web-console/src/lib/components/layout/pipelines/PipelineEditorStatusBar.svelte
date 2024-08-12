<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import type { ProgramStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  let {
    downstreamChanged,
    saveCode,
    programStatus
  }: {
    downstreamChanged: boolean
    saveCode: () => void
    programStatus: ProgramStatus | undefined
  } = $props()

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)
</script>

<div class="flex h-full flex-nowrap">
  <!-- <div class="w-20 p-0.5 text-center">
    {downstreamChanged ? 'changed' : 'saved'}
  </div> -->
  <button
    class={'hover:preset-filled-primary-200-800 px-4 ' +
      (downstreamChanged ? 'preset-tonal-primary' : 'bg-surface-50-950')}
    class:disabled={!downstreamChanged}
    onclick={saveCode}>
    Save
  </button>
  <Tooltip class="text-surface-950-50 z-20 bg-white dark:bg-black">Ctrl + S</Tooltip>
  <button
    class="hover:preset-filled-primary-200-800 w-32 px-2"
    tabindex={10}
    onclick={() => (autoSavePipeline.value = !autoSavePipeline.value)}>
    Autosave: {autoSavePipeline.value ? 'on' : 'off'}
  </button>
</div>
