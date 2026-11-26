<script lang="ts">
  import { ClipboardCopyButton } from 'common-ui'
  import PipelineVersionTooltip from '#lib/components/pipelines/table/PipelineVersionTooltip.svelte'
  import { getRuntimeVersion } from '#lib/functions/pipelines/runtimeVersion.ts'
  import { page } from '$app/state'

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

<!-- The pipeline manager sets the runtime version before the compilation starts. Thus, the
  version shows during a compilation too, and it is the version the program compiles for. -->
{#if version.length < 11}
  {version}
{:else}
  <div class="flex flex-nowrap items-center">
    {version.slice(0, 7)}
    <span class="select-none">...</span>
    <ClipboardCopyButton value={version}></ClipboardCopyButton>
  </div>
{/if}
<!-- The tooltip anchors to its previous sibling, this wrapper. The wrapper keeps the open popover
  in place when a click on Update replaces the chip. -->
<span class="flex">
  {#if status === 'custom'}
    <span
      class="relative chip h-5 rounded-md preset-outlined-surface-200-800 text-sm text-surface-700-300"
    >
      Custom
      <div class="fd fd-info pl-2 text-[14px] text-warning-600-400"></div>
    </span>
  {:else if status === 'update_available'}
    <span
      class="chip h-5 rounded-md preset-outlined-tertiary-800-200 text-sm text-tertiary-800-200"
    >
      Update available
    </span>
  {:else}
    <span class="chip h-5 rounded-md preset-outlined-success-600-400 text-sm"> Latest </span>
  {/if}
</span>
<PipelineVersionTooltip {pipelineName} {status} {baseRuntimeVersion} />
