<script lang="ts">
  let {
    pipelineName,
    status,
    baseRuntimeVersion
  }: {
    pipelineName: string
    status: 'custom' | 'latest' | 'update_available'
    baseRuntimeVersion: string
  } = $props()
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { normalizeRuntimeVersion } from '$lib/functions/pipelines/runtimeVersion'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  let api = usePipelineManager()
</script>

{#if status === 'update_available'}
  <Tooltip
    class="bg-white-dark z-20 rounded-container p-4 text-base text-surface-950-50"
    placement="bottom-end"
    strategy="fixed"
    activeContent
  >
    <div>A new runtime version {normalizeRuntimeVersion(baseRuntimeVersion)} is available.</div>
    <button
      class="btn mt-2 h-6 preset-filled-primary-500"
      onclick={() => api.postUpdateRuntime(pipelineName)}>Update</button
    >
  </Tooltip>
{:else if status === 'custom'}
  <Tooltip
    class="bg-white-dark z-20 rounded-container p-4 text-base text-surface-950-50"
    placement="bottom-end"
    strategy="fixed"
  >
    <div>This custom runtime version is set in the compilation configuration.</div>
  </Tooltip>
{/if}
