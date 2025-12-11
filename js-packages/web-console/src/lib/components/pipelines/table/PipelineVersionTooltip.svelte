<script lang="ts">
  const {
    pipelineName,
    status,
    baseRuntimeVersion
  }: {
    pipelineName: string
    status: 'custom' | 'latest' | 'update_available'
    baseRuntimeVersion: string
  } = $props()

  import { Popover } from '$lib/components/common/Popover.svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { normalizeRuntimeVersion } from '$lib/functions/pipelines/runtimeVersion'

  const api = usePipelineManager()
</script>

{#if status === 'update_available'}
  <Popover placement="bottom-end" strategy="fixed">
    <div>A new runtime version {normalizeRuntimeVersion(baseRuntimeVersion)} is available.</div>
    <button
      class="mt-2 btn h-6 preset-filled-primary-500"
      onclick={() => api.postUpdateRuntime(pipelineName)}>Update</button
    >
  </Popover>
{:else if status === 'custom'}
  <Tooltip placement="bottom-end" strategy="fixed">
    <div>This custom runtime version is set in the compilation configuration.</div>
  </Tooltip>
{/if}
