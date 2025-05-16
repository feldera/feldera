<script lang="ts">
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import { NumberInput } from 'flowbite-svelte'
  import { useCodeEditorSettings } from '$lib/compositions/pipelines/useCodeEditorSettings.svelte'

  const { editorFontSize, autoSavePipeline, showMinimap, showStickyScroll } =
    useCodeEditorSettings()
</script>

<Popup>
  {#snippet trigger(toggle)}
    <button
      class="fd fd-more_horiz btn btn-icon text-[20px] !brightness-100 hover:preset-tonal-surface"
      onclick={toggle}
      aria-label="Editor settings"
    ></button>
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="absolute bottom-14 right-0 z-30 max-h-[400px] w-[calc(100vw-100px)] max-w-[300px]"
    >
      <div class="bg-white-dark flex flex-col justify-center gap-2 rounded-container p-2 shadow-md">
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Autosave
          <Switch name="autoSave" bind:checked={autoSavePipeline.value}></Switch>
        </label>
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Show minimap
          <Switch name="showMinimap" bind:checked={showMinimap.value}></Switch>
        </label>
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Show sticky scroll
          <Switch name="showStickyScroll" bind:checked={showStickyScroll.value}></Switch>
        </label>
        <label
          id="editorFontSize"
          class="flex flex-nowrap items-center justify-between gap-2 whitespace-nowrap p-2"
        >
          Font size
          <NumberInput
            defaultClass="w-fit -m-2 input"
            bind:value={editorFontSize.value}
            min={12}
            max={20}
          />
        </label>
      </div>
    </div>
  {/snippet}
</Popup>
