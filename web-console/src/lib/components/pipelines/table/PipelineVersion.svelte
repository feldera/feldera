<script lang="ts">
  import { page } from '$app/state'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import { getRuntimeVersion } from '$lib/functions/pipelines/runtimeVersion'
  import PipelineVersionTooltip from '$lib/components/pipelines/table/PipelineVersionTooltip.svelte'

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
</script>

{#if status === 'update_available'}
  <div class="fd fd-info pb-0.5 text-[16px] text-blue-500 !ring-blue-500"></div>
{:else if status === 'custom'}
  <div class="fd fd-info pb-0.5 text-[16px] text-warning-500 !ring-warning-500"></div>
{:else}
  <div class="w-4"></div>
{/if}
<PipelineVersionTooltip {pipelineName} {status} {baseRuntimeVersion} />

{#if version.length < 11}
  <span class="text-sm">{version}</span>
{:else}
  <div class="flex flex-nowrap items-center">
    {version.slice(0, 7)}
    <span class="select-none text-sm">...</span>
    <ClipboardCopyButton value={version}></ClipboardCopyButton>
  </div>
{/if}
