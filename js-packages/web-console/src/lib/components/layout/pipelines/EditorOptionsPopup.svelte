<script lang="ts">
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import { Input } from 'flowbite-svelte'
  import { fade } from 'svelte/transition'
  import Popup from '$lib/components/common/Popup.svelte'
  import { useCodeEditorSettings } from '$lib/compositions/pipelines/useCodeEditorSettings.svelte'

  const { editorFontSize, autoSaveFiles, showMinimap, showStickyScroll } = useCodeEditorSettings()
</script>

<Popup wrapperClass="flex">
  {#snippet trigger(toggle)}
    <button
      class="fd fd-more_horiz btn-icon btn-icon-sm text-[16px] hover:bg-surface-50-950 hover:text-surface-950-50"
      onclick={toggle}
      aria-label="Editor settings"
    ></button>
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="absolute right-0 bottom-14 z-30 max-h-[400px] w-[calc(100vw-100px)] max-w-[300px]"
    >
      <div class="bg-white-dark flex flex-col justify-center gap-2 rounded-container p-2 shadow-md">
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Autosave
          <Switch
            class="switch-md"
            name="autoSave"
            checked={autoSaveFiles.value}
            onCheckedChange={(e) => (autoSaveFiles.value = e.checked)}
          >
            <Switch.Control>
              <Switch.Thumb />
            </Switch.Control>
            <Switch.Label />
            <Switch.HiddenInput />
          </Switch>
        </label>
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Show minimap
          <Switch
            class="switch-md"
            name="showMinimap"
            checked={showMinimap.value}
            onCheckedChange={(e) => (showMinimap.value = e.checked)}
          >
            <Switch.Control>
              <Switch.Thumb />
            </Switch.Control>
            <Switch.Label />
            <Switch.HiddenInput />
          </Switch>
        </label>
        <label class="flex cursor-pointer justify-between rounded p-2 hover:preset-tonal-surface">
          Show sticky scroll
          <Switch
            class="switch-md"
            name="showStickyScroll"
            checked={showStickyScroll.value}
            onCheckedChange={(e) => (showStickyScroll.value = e.checked)}
          >
            <Switch.Control>
              <Switch.Thumb />
            </Switch.Control>
            <Switch.Label />
            <Switch.HiddenInput />
          </Switch>
        </label>
        <label
          id="editorFontSize"
          class="flex flex-nowrap items-center justify-between gap-2 p-2 whitespace-nowrap"
        >
          Font size
          <Input
            type="number"
            class="-m-2 input w-fit!"
            bind:value={editorFontSize.value}
            min={12}
            max={20}
          />
        </label>
      </div>
    </div>
  {/snippet}
</Popup>
