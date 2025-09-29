<script lang="ts">
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { nonNull } from '$lib/functions/common/function'
  import type { ProgramStatus } from '$lib/services/pipelineManager'

  let {
    runtimeVersion,
    baseRuntimeVersion,
    configuredRuntimeVersion,
    programStatus
  }: {
    runtimeVersion: string
    baseRuntimeVersion: string
    configuredRuntimeVersion: string | null | undefined
    programStatus: ProgramStatus | undefined
  } = $props()

  let versionStatus = $derived(
    runtimeVersion === baseRuntimeVersion
      ? ('latest' as const)
      : nonNull(configuredRuntimeVersion)
        ? ('custom' as const)
        : ('update_available' as const)
  )
</script>

{#if programStatus === 'Success'}
  {runtimeVersion}
  {#if versionStatus === 'custom'}
    <span class="chip relative h-5 text-sm text-surface-700-300 preset-outlined-surface-200-800">
      Custom
      <div class="fd fd-info pl-2 text-[14px] text-warning-600-400"></div>
    </span>
    <Tooltip
      class="bg-white-dark z-20 w-96 rounded-container p-4 text-base text-surface-950-50"
      placement="bottom-end"
      activeContent
    >
      <div>This runtime version is set in compilation configuration.</div>
    </Tooltip>
  {:else if versionStatus === 'update_available'}
    <span class="chip h-5 text-sm text-blue-500 !ring-blue-500 preset-outlined">
      Update available
    </span>
    <Tooltip
      class="bg-white-dark z-20 w-96 rounded-container p-4 text-base text-surface-950-50"
      placement="bottom-end"
      activeContent
    >
      <div>A newer runtime version {baseRuntimeVersion} is available.</div>
      <!-- <button class="btn mt-2 h-6 preset-filled-primary-500">Update</button> -->
    </Tooltip>
  {:else}
    <span class="chip h-5 text-sm preset-outlined-success-600-400"> Latest </span>
  {/if}
{/if}
