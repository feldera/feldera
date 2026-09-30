<script lang="ts">
  import { ClipboardCopyButton } from 'common-ui'
  import { page } from '$app/state'

  const versionText = $derived(
    page.data.feldera
      ? `Feldera ${page.data.feldera.edition} v${page.data.feldera.version} `
      : undefined
  )
  const revisionText = $derived(
    page.data.feldera ? `(rev. ${page.data.feldera.revision})` : undefined
  )
</script>

<div class="group flex flex-col gap-0.5 text-surface-600-400">
  <span class="pl-8.5">{versionText}</span>
  {#if page.data.feldera}
    <span class="flex items-center justify-between gap-2 pl-8.5 text-sm">
      <span class="break-all">rev. {page.data.feldera.revision}</span>
      <!-- Hidden until hover, but still reachable by keyboard -->
      <ClipboardCopyButton
        class="-my-1 mr-1.5 btn-icon-sm text-[16px] opacity-0 group-hover:opacity-100 hover:bg-surface-50-950 focus-visible:opacity-100"
        value={`${versionText ?? ''}${revisionText ?? ''}`}
      ></ClipboardCopyButton>
    </span>
  {/if}
  {#if page.data.feldera?.update?.version}
    <span class="pl-8.5">latest: {page.data.feldera.update.version}</span>
  {/if}
</div>
