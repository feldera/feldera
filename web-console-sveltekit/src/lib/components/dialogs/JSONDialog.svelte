<script lang="ts">
  import MonacoEditor from '$lib/components/MonacoEditor.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import type { Snippet } from 'svelte'
  let {
    json,
    onApply,
    onClose,
    title
  }: {
    json: string
    onApply: (json: string) => Promise<void>
    onClose: () => void
    title: Snippet
  } = $props()
  const mode = useDarkMode()
  let value = $state(json)
  $effect(() => {
    value = json
  })
</script>

<div class="flex flex-col gap-4 p-4">
  {@render title()}
  <div class="h-96">
    <MonacoEditor
      bind:value
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
        language: 'json'
      }}
    />
  </div>
  <div class="flex w-full justify-end">
    <button onclick={() => onApply(value).then(onClose)} class="btn preset-filled-primary-500">
      APPLY
    </button>
  </div>
</div>
