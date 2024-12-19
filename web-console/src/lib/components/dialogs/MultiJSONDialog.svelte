<script lang="ts">
  import MonacoEditor from '$lib/components/MonacoEditor.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import type { Snippet } from 'svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { useCodeEditorSettings } from '$lib/compositions/pipelines/useCodeEditorSettings.svelte'
  let {
    values,
    metadata,
    onApply,
    onClose,
    title,
    disabled
  }: {
    values: Record<string, string>
    metadata?: Record<string, { title?: string; editorClass?: string }>
    onApply: (values: Record<string, string>) => Promise<void>
    onClose: () => void
    title: Snippet
    disabled?: boolean
  } = $props()
  const theme = useSkeletonTheme()
  const darkMode = useDarkMode()
  let value = $state(values)
  const apply = (value: Record<string, string>) => onApply(value).then(onClose)
  const { editorFontSize, showMinimap, showStickyScroll } = useCodeEditorSettings()
  let keys = $derived(Object.keys(value))
</script>

<div class="flex flex-col gap-4 p-4">
  <div class="flex flex-nowrap justify-between">
    {@render title()}
    <button
      onclick={onClose}
      class="preset-grayout-surface fd fd-x text-[20px]"
      aria-label="Close dialog"
    ></button>
  </div>
  {#each keys as key}
    <span class="h6 font-normal">{metadata?.[key].title ?? ''}</span>
    <div class={metadata?.[key].editorClass}>
      <MonacoEditor
        bind:value={value[key]}
        on:ready={(x) => {
          x.detail.onKeyDown((e) => {
            if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
              apply(value)
              e.preventDefault()
            }
          })
        }}
        options={{
          fontFamily: theme.config.monospaceFontFamily,
          fontSize: editorFontSize.value,
          theme: darkMode.current === 'dark' ? 'feldera-dark' : 'feldera-light',
          automaticLayout: true,
          lineNumbersMinChars: 2,
          overviewRulerLanes: 0,
          hideCursorInOverviewRuler: true,
          overviewRulerBorder: false,
          scrollbar: {
            vertical: 'visible'
          },
          minimap: {
            enabled: showMinimap.value
          },
          stickyScroll: {
            enabled: showStickyScroll.value
          },
          language: 'json',
          ...isMonacoEditorDisabled(disabled)
        }}
      />
    </div>
  {/each}
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
