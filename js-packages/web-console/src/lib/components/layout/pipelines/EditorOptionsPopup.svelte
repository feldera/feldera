<script lang="ts">
  import { Input } from 'flowbite-svelte'
  import { fade } from 'svelte/transition'
  import CompactSwitch from '#lib/components/common/CompactSwitch.svelte'
  import Popup from '#lib/components/common/Popup.svelte'
  import { useCodeEditorSettings } from '#lib/compositions/pipelines/useCodeEditorSettings.svelte.ts'

  const { editorFontSize, autoSaveFiles, showMinimap, showStickyScroll } = useCodeEditorSettings()
</script>

<Popup wrapperClass="flex">
  {#snippet trigger(toggle)}
    <button
      class="fd fd-more_horiz btn-icon btn-icon-sm text-[16px] hover:bg-row-hover hover:text-surface-950-50"
      onclick={toggle}
      aria-label="Editor settings"
    ></button>
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="absolute right-0 bottom-full z-30 mb-1 max-h-[400px] w-[calc(100vw-100px)] max-w-[300px]"
    >
      <div
        class="bg-white-dark flex flex-col justify-center rounded p-1 shadow-md dark:border dark:border-surface-950-50/10"
      >
        <label
          class="flex h-9 cursor-pointer items-center justify-between gap-2 rounded px-3 hover:bg-row-hover"
        >
          Autosave
          <CompactSwitch
            size="md"
            name="autoSave"
            checked={autoSaveFiles.value}
            onCheckedChange={(e) => (autoSaveFiles.value = e.checked)}
          />
        </label>
        <label
          class="flex h-9 cursor-pointer items-center justify-between gap-2 rounded px-3 hover:bg-row-hover"
        >
          Show minimap
          <CompactSwitch
            size="md"
            name="showMinimap"
            checked={showMinimap.value}
            onCheckedChange={(e) => (showMinimap.value = e.checked)}
          />
        </label>
        <label
          class="flex h-9 cursor-pointer items-center justify-between gap-2 rounded px-3 hover:bg-row-hover"
        >
          Show sticky scroll
          <CompactSwitch
            size="md"
            name="showStickyScroll"
            checked={showStickyScroll.value}
            onCheckedChange={(e) => (showStickyScroll.value = e.checked)}
          />
        </label>
        <label
          id="editorFontSize"
          class="flex h-9 flex-nowrap items-center justify-between gap-2 px-3 whitespace-nowrap"
        >
          Font size
          <Input
            type="number"
            class="input h-7 w-fit! py-0"
            bind:value={editorFontSize.value}
            min={12}
            max={20}
          />
        </label>
      </div>
    </div>
  {/snippet}
</Popup>
