<script lang="ts">
  import MonacoEditor from '$lib/components/MonacoEditor.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import type { Snippet } from 'svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  let {
    json,
    onApply,
    onClose,
    title,
    disabled
  }: {
    json: string
    onApply: (json: string) => Promise<void>
    onClose: () => void
    title: Snippet
    disabled?: boolean
  } = $props()
  const mode = useDarkMode()
  let value = $state(json)
  $effect(() => {
    value = json
  })
  const apply = (value: string) => onApply(value).then(onClose)
</script>

<div class="flex flex-col gap-4 p-4">
  <div class="flex flex-nowrap justify-between">
    {@render title()}
    <button
      onclick={onClose}
      class="preset-grayout-surface fd fd-close text-[24px]"
      aria-label="Close dialog"
    ></button>
  </div>
  <div class="h-96">
    <MonacoEditor
      bind:value
      on:ready={(x) => {
        x.detail.onKeyDown((e) => {
          if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
            apply(value)
            e.preventDefault()
          }
        })
      }}
      options={{
        theme: mode.darkMode.value === 'light' ? 'vs' : 'vs-dark',
        automaticLayout: true,
        lineNumbersMinChars: 2,
        overviewRulerLanes: 0,
        hideCursorInOverviewRuler: true,
        overviewRulerBorder: false,
        minimap: { enabled: false },
        scrollbar: {
          vertical: 'visible'
        },
        language: 'json',
        ...isMonacoEditorDisabled(disabled)
      }}
    />
  </div>
  <div class="flex w-full justify-end">
    <div>
      <button {disabled} onclick={() => apply(value)} class="btn preset-filled-primary-500">
        APPLY
      </button>
    </div>
    {#if disabled}
      <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="top">
        Stop the pipeline to edit configuration
      </Tooltip>
    {/if}
  </div>
</div>
