<script lang="ts">
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'

  let {
    runtimeVersion,
    baseRuntimeVersion
  }: {
    runtimeVersion: string
    baseRuntimeVersion: string
  } = $props()

  let versionStatus = $derived(
    runtimeVersion === baseRuntimeVersion ? ('latest' as const) : ('update_available' as const)
  )
</script>

{runtimeVersion}
{#if versionStatus === 'update_available'}
  <div class="fd fd-info pb-0.5 text-[16px] text-blue-500 !ring-blue-500"></div>
  <Tooltip
    class="bg-white-dark z-20 rounded-container p-4 text-base text-surface-950-50"
    placement="bottom-end"
    strategy="fixed"
    activeContent
  >
    <div>A newer runtime version {baseRuntimeVersion} is available.</div>
    <!-- <button class="btn mt-2 h-6 preset-filled-primary-500">Update</button> -->
  </Tooltip>
{:else}
  <div class="w-5"></div>
{/if}
