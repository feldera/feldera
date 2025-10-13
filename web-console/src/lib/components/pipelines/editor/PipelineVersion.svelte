<script lang="ts">
  import { page } from '$app/state'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import PipelineVersionTooltip from '$lib/components/pipelines/table/PipelineVersionTooltip.svelte'
  import { getRuntimeVersion } from '$lib/functions/pipelines/runtimeVersion'
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
</script>

{#if programStatus === 'Success' || programStatus === 'SystemError' || programStatus === 'SqlError' || programStatus === 'SqlCompiled' || programStatus === 'RustError'}
  {#if version.length < 11}
    {version}
  {:else}
    <div class="flex flex-nowrap items-center">
      {version.slice(0, 7)}
      <span class="select-none">...</span>
      <ClipboardCopyButton value={version}></ClipboardCopyButton>
    </div>
  {/if}
  {#if status === 'custom'}
    <span class="chip relative h-5 text-sm text-surface-700-300 preset-outlined-surface-200-800">
      Custom
      <div class="fd fd-info pl-2 text-[14px] text-warning-600-400"></div>
    </span>
  {:else if status === 'update_available'}
    <span class="chip h-5 text-sm text-blue-500 !ring-blue-500 preset-outlined">
      Update available
    </span>
  {:else}
    <span class="chip h-5 text-sm preset-outlined-success-600-400"> Latest </span>
  {/if}
{/if}
<PipelineVersionTooltip {pipelineName} {status} {baseRuntimeVersion} />
