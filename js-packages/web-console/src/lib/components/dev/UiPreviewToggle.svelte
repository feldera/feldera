<script lang="ts">
  // Dev-only floating switch between UI states, see `useUiPreview`.
  import { useUiPreview } from '$lib/compositions/dev/uiPreview.svelte'

  const preview = useUiPreview()
  const options = [
    { label: 'With pipelines', empty: false },
    { label: 'No pipelines', empty: true }
  ]
</script>

<div
  class="fixed bottom-4 left-4 z-[9999] flex items-center gap-1 rounded-lg border border-surface-950-50/10 bg-white p-1 text-sm shadow-lg dark:bg-dark"
>
  <span class="px-2 text-surface-500">Preview</span>
  {#each options as o}
    <button
      class="rounded-md px-2 py-1 {preview.noPipelines === o.empty
        ? 'bg-primary-500 text-primary-contrast-500'
        : 'hover:bg-surface-100-900'}"
      onclick={() => (preview.noPipelines = o.empty)}
    >
      {o.label}
    </button>
  {/each}
</div>
