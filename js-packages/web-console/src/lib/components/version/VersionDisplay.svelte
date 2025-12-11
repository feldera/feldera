<script lang="ts">
  import { page } from '$app/state'
  import { Popover } from '$lib/components/common/Popover.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'

  const versionText = $derived(
    page.data.feldera
      ? `Feldera ${page.data.feldera.edition} v${page.data.feldera.version} `
      : undefined
  )
  const revisionText = $derived(
    page.data.feldera ? `(rev. ${page.data.feldera.revision})` : undefined
  )
</script>

<div class="relative hr flex justify-between pt-4 text-surface-500">
  <span>{versionText}</span>
  {#if revisionText}
    <Popover
      class="bg-white-dark z-10 mt-16 -mr-20 -ml-4 w-full max-w-[400px] pt-2 pl-2"
      placement="top-start"
    >
      {versionText}
      <ClipboardCopyButton class="absolute top-0 right-0 m-2" value={versionText + revisionText}
      ></ClipboardCopyButton>
      <br class="select-none" />
      <span class="text-nowrap">{revisionText}</span>
    </Popover>
  {/if}
  {#if page.data.feldera?.update?.version}
    <span>latest: {page.data.feldera.update.version}</span>
  {/if}
</div>
