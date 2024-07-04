<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import MonacoEditor from '$lib/functions/common/monacoEditor'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import type { Readable, Writable, WritableLoadable } from '@square/svelte-store'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { useDebounce } from 'runed'
  import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import PipelineEditorStatusBar from './PipelineEditorStatusBar.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { asyncDecoupled } from '$lib/compositions/asyncDecoupled.svelte'
  import { useChangedPipelines } from '$lib/compositions/pipelines/useChangedPipelines.svelte'

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)

  const {
    pipelineName,
    pipelineCodeStore
  }: { pipelineName?: Readable<string>; pipelineCodeStore: WritableLoadable<string> } = $props()

  /// const debouncedCode = asyncDebounced(pipelineCodeStore, 1000)
  const decoupledCode = asyncDecoupled(
    pipelineCodeStore,
    autoSavePipeline.value ? 1000 : 'decoupled'
  )
  const changedPipelines = useChangedPipelines()
  $effect(() => {
    autoSavePipeline.value ? decoupledCode.debounce(1000) : decoupledCode.decouple()
  })
  $effect(() => {
    if (!decoupledCode.upstreamChanged) {
      return
    }
    decoupledCode.pull()
  })
  $effect(() => {
    if (!$pipelineName) {
      return
    }
    decoupledCode.downstreamChanged
      ? changedPipelines.add($pipelineName)
      : changedPipelines.remove($pipelineName)
  })

  {
    let oldPipelineName = $state($pipelineName)
    $effect(() => {
      if ($pipelineName === oldPipelineName) {
        return
      }
      changedPipelines.remove(oldPipelineName || '')
      oldPipelineName = $pipelineName
    })
  }
  const mode = useDarkMode()
</script>

<div class="h-full">
  <PaneGroup direction="vertical" class="">
    <Pane defaultSize={50} minSize={20}>
      <PaneGroup direction="horizontal" autoSaveId="layout/pipelines/vertical/pos">
        <Pane defaultSize={20} minSize={20}>
          <div class="mr-1 bg-white dark:bg-black">List of connectors</div>
        </Pane>
        <PaneResizer class="w-4" />
        <Pane defaultSize={80} minSize={50} class="flex flex-col-reverse">
          <PipelineEditorStatusBar downstreamChanged={decoupledCode.downstreamChanged}
          ></PipelineEditorStatusBar>
          <div class="h-full w-full">
            <MonacoEditor
              on:ready={(x) =>
                x.detail.onKeyDown((e) => {
                  if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
                    decoupledCode.push()
                    e.preventDefault()
                  }
                })}
              bind:value={$decoupledCode}
              options={{
                theme: mode.darkMode.value === 'light' ? 'vs' : 'vs-dark',
                automaticLayout: true,
                lineNumbersMinChars: 3,
                ...isMonacoEditorDisabled(false),
                overviewRulerLanes: 0,
                hideCursorInOverviewRuler: true,
                overviewRulerBorder: false,
                scrollbar: {
                  vertical: 'visible'
                }
              }}
            />
          </div>
        </Pane>
      </PaneGroup>
    </Pane>
    <PaneResizer class="h-4" />
    <Pane defaultSize={50} minSize={30} class="!overflow-visible">
      {#if $pipelineName}
        <InteractionsPanel pipelineName={$pipelineName}></InteractionsPanel>
      {/if}
    </Pane>
  </PaneGroup>
</div>
