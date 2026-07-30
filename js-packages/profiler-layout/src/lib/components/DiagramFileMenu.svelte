<script lang="ts">
  // "File" dropdown menu for the graph-panel header. Owns its open/close state
  // and the "Export as .svg" action.
  import { downloadFile } from '../functions/download'
  import { profileImageFileName } from '../functions/fileName'

  let {
    exportSvg,
    onError,
    pipelineName,
    snapshotDate
  }: {
    /** Serialize the current diagram to an SVG document, or null if nothing is rendered yet. */
    exportSvg: () => Promise<string | null>
    /** Report a user-facing error (nothing to export, serialization failed). */
    onError: (message: string) => void
    /** Pipeline name, used in the exported file name. */
    pipelineName?: string
    /** Profile snapshot date, used in the exported file name (falls back to today). */
    snapshotDate?: Date | null
  } = $props()

  let open = $state(false)
  let menuEl: HTMLDivElement | undefined = $state()

  async function exportImage() {
    open = false
    try {
      const svg = await exportSvg()
      if (!svg) {
        onError('No diagram to export yet.')
        return
      }
      downloadFile(svg, profileImageFileName(pipelineName, snapshotDate ?? new Date()), 'image/svg+xml')
    } catch (e) {
      onError(`Failed to export image: ${e instanceof Error ? e.message : String(e)}`)
    }
  }
</script>

<!-- Close the menu on an outside click or Escape. -->
<svelte:window
  onclick={(e) => {
    if (open && menuEl && !menuEl.contains(e.target as Node)) {
      open = false
    }
  }}
  onkeydown={(e) => {
    if (e.key === 'Escape') {
      open = false
    }
  }}
/>

<div class="relative" bind:this={menuEl}>
  <button
    class="btn h-6 !bg-surface-100-900 px-3 text-sm outline-none"
    onclick={() => (open = !open)}
    aria-haspopup="menu"
    aria-expanded={open}
  >
    File
  </button>
  {#if open}
    <div class="absolute top-8 left-0 z-30 w-max min-w-[180px]" role="menu">
      <div class="bg-white-dark flex flex-col overflow-hidden rounded shadow-md">
        <button
          class="px-4 py-2 text-left text-sm hover:preset-tonal-surface"
          role="menuitem"
          onclick={exportImage}
        >
          Export as .svg
        </button>
      </div>
    </div>
  {/if}
</div>
