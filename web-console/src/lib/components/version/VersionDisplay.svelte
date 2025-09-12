<script lang="ts">
  import { page } from '$app/state'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'

  let versionText = page.data.feldera
    ? `Feldera ${page.data.feldera.edition} v${page.data.feldera.version} `
    : undefined
  let revisionText = page.data.feldera ? `(rev. ${page.data.feldera.revision})` : undefined
</script>

<div class="hr flex justify-between pt-4 text-surface-500">
  <span>{versionText}</span>
  {#if revisionText}
    <Tooltip
      class="z-10 -ml-4 mt-16 w-full rounded-container bg-white p-4 text-base font-normal text-surface-950-50 dark:bg-black"
      placement="top-start"
      activeContent
    >
      {versionText}
      <ClipboardCopyButton class="absolute right-0 top-0 m-2" value={versionText + revisionText}
      ></ClipboardCopyButton>
      <br class="select-none" />
      <span class="text-nowrap">{revisionText}</span>
    </Tooltip>
  {/if}
  {#if page.data.feldera?.update?.version}
    <span>latest: v{page.data.feldera.update.version}</span>
  {/if}
</div>
