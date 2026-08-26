<script lang="ts">
  // Shared dropdown body for the support-bundle menus: an optional download button
  // with the "collect new data" toggle, and the entry that opens a bundle from disk.
  //
  // Callers supply their own Popup and outer container, so the trigger and the outer
  // styling stay caller-specific. Callers also do the picking: each one has its own
  // use for a picked bundle, and the hidden file input the fallback needs has to sit
  // outside this menu, where a closing popup cannot remove it mid-pick.

  type Props = {
    collectNewData?: boolean
    /** When omitted, the menu offers no download and no "collect new data" toggle. */
    onDownload?: () => void
    /** Opens a bundle from disk: the file picker, or a file input as a fallback. */
    onPickBundle: () => void
    disabled?: boolean
    downloadLabel?: string
    pickLabel?: string
  }

  let {
    collectNewData = $bindable(false),
    onDownload,
    onPickBundle,
    disabled = false,
    downloadLabel = 'Download support bundle',
    pickLabel = 'Open support bundle'
  }: Props = $props()
</script>

{#if onDownload}
  <button
    class="px-4 py-2 text-left hover:preset-tonal-surface"
    onclick={onDownload}
    {disabled}
    data-testid="btn-download-support-bundle"
  >
    {downloadLabel}
  </button>

  <label
    class="flex cursor-pointer items-center justify-between gap-3 px-4 py-2 hover:preset-tonal-surface"
  >
    <span>Collect new data</span>
    <input type="checkbox" bind:checked={collectNewData} class="checkbox" />
  </label>

  <div class="hr"></div>
{/if}

<button
  class="px-4 py-2 text-left hover:preset-tonal-surface"
  onclick={onPickBundle}
  {disabled}
  data-testid="btn-upload-support-bundle"
>
  {pickLabel}
</button>
