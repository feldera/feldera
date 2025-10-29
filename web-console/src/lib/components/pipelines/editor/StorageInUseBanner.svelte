<script lang="ts">
  import { page } from '$app/state'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  let {
    pipelineName,
    runtimeVersion
  }: {
    pipelineName: string
    runtimeVersion: { version: string; status: 'latest' | 'update_available' | 'custom' }
  } = $props()

  const api = usePipelineManager()

  async function handleUpgrade() {
    api.postUpdateRuntime(pipelineName)
  }
</script>

<div class="flex h-10 items-center gap-2 px-4 preset-tonal-tertiary">
  <span class="fd fd-triangle-alert text-[20px]"></span>
  {#if runtimeVersion.status === 'update_available'}
    <span>
      A new runtime version {page.data.feldera!.version} is available. Update the runtime to edit the pipeline.
    </span>
    <button class="btn h-7 border-0 py-0 text-sm preset-filled-primary-500" onclick={handleUpgrade}>
      Update
    </button>
  {:else}
    <span>
      Editing a pipeline with existing state in storage — changes will take effect when the
      pipeline restarts. See the <a href="https://docs.feldera.com/pipelines/modifying" target="_blank" rel="noopener noreferrer">documentation</a>.
    </span>
  {/if}
</div>
