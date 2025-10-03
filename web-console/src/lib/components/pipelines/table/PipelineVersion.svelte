<script lang="ts">
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import {
    getRuntimeVersionStatus,
    normalizeRuntimeVersion
  } from '$lib/functions/pipelines/runtimeVersion'

  let {
    runtimeVersion,
    baseRuntimeVersion,
    configuredRuntimeVersion
  }: {
    runtimeVersion: string
    baseRuntimeVersion: string
    configuredRuntimeVersion: string | null | undefined
  } = $props()

  let versionStatus = $derived(
    getRuntimeVersionStatus({
      runtime: runtimeVersion,
      base: baseRuntimeVersion,
      configured: configuredRuntimeVersion
    })
  )
</script>

{#if versionStatus === 'update_available'}
  <div class="fd fd-info pb-0.5 text-[16px] text-blue-500 !ring-blue-500"></div>
  <Tooltip
    class="bg-white-dark z-20 rounded-container p-4 text-base text-surface-950-50"
    placement="bottom-end"
    strategy="fixed"
    activeContent
  >
    <div>A newer runtime version {normalizeRuntimeVersion(baseRuntimeVersion)} is available.</div>
    <!-- <button class="btn mt-2 h-6 preset-filled-primary-500">Update</button> -->
  </Tooltip>
{:else if versionStatus === 'custom'}
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
<span class="text-sm">{normalizeRuntimeVersion(runtimeVersion)}</span>
