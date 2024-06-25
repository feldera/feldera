<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import MonacoEditor from 'svelte-monaco'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import type { Readable, Writable } from '@square/svelte-store'

  const {
    pipelineName,
    pipelineCodeStore
  }: { pipelineName?: Readable<string>; pipelineCodeStore: Writable<string> } = $props()

  const mode = useDarkMode()
</script>

<div class="h-full">
  <SplitPane type="vertical" id="main" min="100px" max="70%" pos="50%" priority="min">
    <section slot="a">
      <SplitPane type="horizontal" id="main" min="200px" max="-100px" pos="20%" priority="min">
        <section slot="a">
          <div class="mr-1 bg-white dark:bg-black">List of connectors</div>
        </section>
        <section slot="b" class="ml-1 bg-white dark:bg-black">
          <MonacoEditor
            bind:value={$pipelineCodeStore}
            options={{ theme: mode.darkMode.value === 'light' ? 'vs' : 'vs-dark' }} />
        </section>
      </SplitPane>
    </section>
    <section slot="b">
      {#if $pipelineName}
        <InteractionsPanel pipelineName={$pipelineName}></InteractionsPanel>
      {/if}
    </section>
  </SplitPane>
</div>
