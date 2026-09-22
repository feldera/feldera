<script lang="ts">
  // The body shared by the two support bundle dropdowns: an optional download button
  // with its "collect new data" toggle, and the entry that opens a bundle from disk.
  //
  // The caller supplies the Popup and the container around it, so that the trigger and
  // the styling stay the caller's. The caller also does the picking itself, for two
  // reasons: each one does something different with the bundle that comes back, and
  // the `<input type=file>` needed where the browser has no `showOpenFilePicker` has
  // to sit outside this menu, because a dropdown that closes would unmount the input
  // while the user is still choosing a file in it.

  type Props = {
    collectNewData?: boolean
    /** When omitted, the menu offers no download and no "collect new data" toggle. */
    onDownload?: () => void
    /**
     * Runs the caller's own picking, with `showOpenFilePicker` or with an
     * `<input type=file>` where that is missing.
     */
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
