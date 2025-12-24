<script lang="ts">
  import { page } from '$app/state'
  import FelderaModernLogomarkBlack from '$assets/images/feldera-modern/Feldera Logomark Black.svg?component'
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

{#snippet logo(className: string)}
  <FelderaModernLogomarkBlack class="inline w-5 {className}"></FelderaModernLogomarkBlack>
{/snippet}

<div class="relative text-surface-600-400">
  <span class="flex gap-2">{@render logo('fill-surface-600-400')} {versionText}</span>
  {#if revisionText}
    <Popover
      class="bg-white-dark z-10 -mt-11 -mr-20 ml-4 w-full max-w-[400px] pt-2 pl-2 text-surface-950-50"
      placement="bottom-end"
    >
      <span class="flex gap-2">{@render logo('fill-surface-950-50')} {versionText}</span>
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
