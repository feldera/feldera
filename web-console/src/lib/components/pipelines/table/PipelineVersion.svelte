<script lang="ts">
  import { page } from '$app/state'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import {
    getRuntimeVersion,
    normalizeRuntimeVersion
  } from '$lib/functions/pipelines/runtimeVersion'

  let {
    pipelineName,
    runtimeVersion,
    baseRuntimeVersion,
    configuredRuntimeVersion
  }: {
    pipelineName: string
    runtimeVersion: string
    baseRuntimeVersion: string
    configuredRuntimeVersion: string | null | undefined
  } = $props()

  let { version, status } = $derived(
    getRuntimeVersion(
      {
        runtime: runtimeVersion,
        base: baseRuntimeVersion,
        configured: configuredRuntimeVersion
      },
      page.data.feldera!.unstableFeatures
    )
  )

  let api = usePipelineManager()
</script>

{#if status === 'update_available'}
  <div class="fd fd-info pb-0.5 text-[16px] text-blue-500 !ring-blue-500"></div>
  <Tooltip
    class="bg-white-dark z-20 rounded-container p-4 text-base text-surface-950-50"
    placement="bottom-end"
    strategy="fixed"
    activeContent
  >
    <div>A newer runtime version {normalizeRuntimeVersion(baseRuntimeVersion)} is available.</div>
    <button
      class="btn mt-2 h-6 preset-filled-primary-500"
      onclick={() => api.postUpdateRuntime(pipelineName)}>Update</button
    >
  </Tooltip>
{:else if status === 'custom'}
  <div class="fd fd-info pb-0.5 text-[16px] text-warning-500 !ring-warning-500"></div>
  <Tooltip
    class="bg-white-dark z-20 rounded-container p-4 text-base text-surface-950-50"
    placement="bottom-end"
    strategy="fixed"
    activeContent
  >
    <div>This custom runtime version is set in the compilation configuration.</div>
  </Tooltip>
{:else}
  <div class="w-5"></div>
{/if}
<span class="text-sm">{version}</span>
