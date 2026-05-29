<script lang="ts">
  // Shared dropdown body for the support-bundle menus: a download button, the
  // "collect new data" toggle, a divider, and an upload button (with its own
  // hidden file input). Callers wrap this in their own Popup + outer container
  // so the trigger and outer styling stay caller-specific.
  //
  // The hidden file input is rendered here on purpose: when this menu lives
  // inside a Popup's content snippet, the input ends up inside the popup's
  // `contentNode`, and the synthetic click that `fileInput.click()` dispatches
  // is treated as an inside-popup click — so the popup doesn't auto-close on
  // upload (see `Popup.svelte`'s window-level capture handler).

  type Props = {
    collectNewData: boolean
    onDownload: () => void
    onFilePicked: (file: File) => void
    disabled?: boolean
    downloadLabel?: string
    uploadLabel?: string
  }

  let {
    collectNewData = $bindable(),
    onDownload,
    onFilePicked,
    disabled = false,
    downloadLabel = 'Download support bundle',
    uploadLabel = 'Upload support bundle'
  }: Props = $props()

  let fileInput: HTMLInputElement | null = $state(null)
</script>

<input
  type="file"
  accept=".zip"
  bind:this={fileInput}
  onchange={(e) => {
    const file = (e.currentTarget as HTMLInputElement).files?.[0]
    if (file) {
      ;(e.currentTarget as HTMLInputElement).value = ''
      onFilePicked(file)
    }
  }}
  class="hidden"
  data-testid="input-upload-support-bundle"
/>

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

<button
  class="px-4 py-2 text-left hover:preset-tonal-surface"
  onclick={() => fileInput?.click()}
  {disabled}
  data-testid="btn-upload-support-bundle"
>
  {uploadLabel}
</button>
