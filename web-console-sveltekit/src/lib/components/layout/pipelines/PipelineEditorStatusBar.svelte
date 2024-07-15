<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'

  let { downstreamChanged }: { downstreamChanged: boolean } = $props()

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)
</script>

<div class="flex h-full flex-nowrap gap-2">
  <!-- <div class="w-20 p-0.5 text-center">
    {downstreamChanged ? 'changed' : 'saved'}
  </div> -->
  <button
    class="w-32 px-2 hover:preset-filled-primary-50-950"
    tabindex={10}
    onclick={() => (autoSavePipeline.value = !autoSavePipeline.value)}
  >
    autosave: {autoSavePipeline.value ? 'on' : 'off'}
  </button>
</div>
