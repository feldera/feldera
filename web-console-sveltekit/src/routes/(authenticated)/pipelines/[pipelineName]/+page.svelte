<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import InteractionsPanel from '$lib/components/pipelines/InteractionsPanel.svelte'
  import { page } from '$app/stores'
  import { localStore } from '$lib/compositions/localStore.svelte'
  import { onMount } from 'svelte'
  import { writablePipeline } from '$lib/compositions/pipelineManager'
  import { asyncWritable, derived, readable } from '@square/svelte-store'
  import { useDebounce } from '$lib/compositions/debounce.svelte'
  import MonacoEditor from 'svelte-monaco'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'

  let pipelineName = derived(page, (page) => decodeURI(page.params.pipelineName))
  {
    let openPipelines = localStore<string[]>('pipelines/open', [])
    onMount(() => {
      if (!openPipelines.value.includes($pipelineName)) {
        openPipelines.value = [...openPipelines.value, $pipelineName]
      }
    })
  }

  const pipeline = writablePipeline(pipelineName)
  const debounce = useDebounce()
  const debouncedPipeline = asyncWritable(
    pipeline,
    (p) => p,
    async (p) => {
      debounce((p) => {
        $pipeline = p
      }, 1000)(p)
      return p
    }
  )
  const pipelineCodeStore = asyncWritable(
    pipeline!,
    (pipeline) => pipeline.code,
    async (newCode, pipeline, oldCode) => {
      if (!pipeline || !oldCode || !newCode) {
        return oldCode
      }
      $debouncedPipeline = {
        ...pipeline,
        code: newCode
      }
      return newCode
    }
  )
  const mode = useDarkMode()
</script>

<div class=" h-full">
  <SplitPane type="vertical" id="main" min="100px" max="70%" pos="50%" priority="min">
    <section slot="a">
      <SplitPane type="horizontal" id="main" min="200px" max="-100px" pos="20%" priority="min">
        <section slot="a">
          <div class="mr-1 bg-white dark:bg-black">List of connectors</div>
        </section>
        <section slot="b" class="ml-1 bg-white dark:bg-black">
          <MonacoEditor
            bind:value={$pipelineCodeStore}
            options={{ theme: 'vs-' + mode.darkMode.value }} />
        </section>
      </SplitPane>
    </section>
    <section slot="b">
      <InteractionsPanel pipelineName={$pipelineName}></InteractionsPanel>
    </section>
  </SplitPane>
</div>

<style>
</style>
