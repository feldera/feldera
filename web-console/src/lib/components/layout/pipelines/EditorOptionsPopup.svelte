<script lang="ts">
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import JSONDialog from '$lib/components/dialogs/JSONDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { NumberInput } from 'flowbite-svelte'
  import { useCodeEditorSettings } from '$lib/compositions/pipelines/useCodeEditorSettings.svelte'

  const { editorFontSize, autoSavePipeline, showMinimap, showStickyScroll } =
    useCodeEditorSettings()
</script>

<Popup>
  {#snippet trigger(toggle)}
    <button
      class="fd fd-settings btn btn-icon text-[24px] preset-tonal-surface"
      onclick={toggle}
      aria-label="Pipeline actions"
    ></button>
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="absolute left-0 z-10 max-h-[400px] w-[calc(100vw-100px)] max-w-[300px]"
    >
      <div
        class="bg-white-black flex flex-col justify-center gap-2 rounded-container p-2 shadow-md"
      >
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Show minimap
          <Switch name="showMinimap" bind:checked={showMinimap.value}></Switch>
        </label>
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Show sticky scroll
          <Switch name="showStickyScroll" bind:checked={showStickyScroll.value}></Switch>
        </label>
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Autosave
          <Switch name="autoSave" bind:checked={autoSavePipeline.value}></Switch>
        </label>
        <label
          id="editorFontSize"
          class="flex flex-nowrap items-center justify-between gap-2 whitespace-nowrap p-2"
        >
          Font size
          <NumberInput
            defaultClass="w-fit -m-2"
            bind:value={editorFontSize.value}
            min={12}
            max={20}
          />
        </label>
      </div>
    </div>
  {/snippet}
</Popup>
