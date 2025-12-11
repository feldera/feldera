<script lang="ts">
  import { page } from '$app/state'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import PipelineVersionTooltip from '$lib/components/pipelines/table/PipelineVersionTooltip.svelte'
  import { getRuntimeVersion } from '$lib/functions/pipelines/runtimeVersion'

  const {
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

{#if status === 'update_available'}
  <div class="fd fd-info pb-0.5 text-[20px] text-tertiary-700-300"></div>
{:else if status === 'custom'}
  <div class="fd fd-info pb-0.5 text-[20px] text-warning-500"></div>
{:else}
  <div class="w-5"></div>
{/if}
<PipelineVersionTooltip {pipelineName} {status} {baseRuntimeVersion} />

{#if version.length < 11}
  <span class="text-sm">{version}</span>
{:else}
  <div class="flex flex-nowrap items-center">
    {version.slice(0, 7)}
    <span class="text-sm select-none">...</span>
    <ClipboardCopyButton value={version}></ClipboardCopyButton>
  </div>
{/if}
