<script lang="ts">
  import { page } from '$app/state'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import PipelineVersionTooltip from '$lib/components/pipelines/table/PipelineVersionTooltip.svelte'
  import { getRuntimeVersion } from '$lib/functions/pipelines/runtimeVersion'
  import type { ProgramStatus } from '$lib/services/pipelineManager'

  const {
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

  const { version, status } = $derived(
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
    <span class="relative chip h-5 preset-outlined-surface-200-800 text-sm text-surface-700-300">
      Custom
      <div class="fd fd-info pl-2 text-[14px] text-warning-600-400"></div>
    </span>
  {:else if status === 'update_available'}
    <span class="chip h-5 preset-outlined-tertiary-800-200 text-sm text-tertiary-800-200">
      Update available
    </span>
  {:else}
    <span class="chip h-5 preset-outlined-success-600-400 text-sm"> Latest </span>
  {/if}
{/if}
<PipelineVersionTooltip {pipelineName} {status} {baseRuntimeVersion} />
