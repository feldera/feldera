<script lang="ts">
  import { page } from '$app/state'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import {
    getRuntimeVersion,
    normalizeRuntimeVersion
  } from '$lib/functions/pipelines/runtimeVersion'
  import type { ProgramStatus } from '$lib/services/pipelineManager'

  let {
    pipelineName,
    runtimeVersion,
    baseRuntimeVersion,
    configuredRuntimeVersion,
    programStatus
  }: {
    pipelineName: string
    runtimeVersion: string
    baseRuntimeVersion: string
    configuredRuntimeVersion: string | null | undefined
    programStatus: ProgramStatus | undefined
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

  const api = usePipelineManager()
</script>

{#if programStatus === 'Success'}
  {version}
  {#if status === 'custom'}
    <span class="chip relative h-5 text-sm text-surface-700-300 preset-outlined-surface-200-800">
      Custom
      <div class="fd fd-info pl-2 text-[14px] text-warning-600-400"></div>
    </span>
    <Tooltip
      class="bg-white-dark z-20 w-96 rounded-container p-4 text-base text-surface-950-50"
      placement="bottom-end"
      activeContent
    >
      <div>This custom runtime version is set in the compilation configuration.</div>
    </Tooltip>
  {:else if status === 'update_available'}
    <span class="chip h-5 text-sm text-blue-500 !ring-blue-500 preset-outlined">
      Update available
    </span>
    <Tooltip
      class="bg-white-dark z-20 w-96 rounded-container p-4 text-base text-surface-950-50"
      placement="bottom-end"
      activeContent
    >
      <div>A newer runtime version {normalizeRuntimeVersion(baseRuntimeVersion)} is available.</div>
      <button
        class="btn mt-2 h-6 preset-filled-primary-500"
        onclick={() => api.postUpdateRuntime(pipelineName)}>Update</button
      >
    </Tooltip>
  {:else}
    <span class="chip h-5 text-sm preset-outlined-success-600-400"> Latest </span>
  {/if}
{/if}
