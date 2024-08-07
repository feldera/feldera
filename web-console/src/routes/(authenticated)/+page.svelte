<script lang="ts">
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import type { PipelineDescr } from '$lib/services/manager'
  import { postPipeline } from '$lib/services/pipelineManager'

  let { data } = $props()

  const pipelines = usePipelineList()
  const tryPipelineFromExample = async (example: PipelineDescr) => {
    if (!pipelines.pipelines.some((p) => p.name === example.name)) {
      const newPipeline = await postPipeline({
        name: example.name,
        runtime_config: example.runtime_config,
        program_config: example.program_config,
        description: example.description,
        program_code: example.program_code
      })
      pipelines.pipelines.push(newPipeline)
    }
    goto(`${base}/pipelines/${encodeURIComponent(example.name)}/`)
  }
</script>

<div class="self-center">
  {#if data.demos.length}
    <div class="h5 px-8 py-8 font-normal">Try running one of our examples below.</div>
    <div
      class="grid max-w-[1390px] grid-cols-1 gap-8 px-8 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4"
    >
      {#each data.demos as demo}
        <div class="card flex flex-col gap-2 bg-white p-4 dark:bg-black">
          <button
            class="text-left text-primary-500"
            onclick={() => tryPipelineFromExample(demo.pipeline)}
          >
            <span class="text-lg">{demo.title}</span>
            <span class="bx bx-right-arrow-alt w-0 translate-y-0.5 scale-150"></span>
          </button>
          <span class="text-left">{demo.pipeline.description}</span>
        </div>
      {/each}
    </div>
  {:else}
    <div class="h5 px-8 py-8 font-normal">
      Write a new streaming SQL query from scratch:
      <button
        class="btn mt-auto self-end text-sm preset-filled-primary-500"
        onclick={() => goto('#new')}
      >
        CREATE NEW PIPELINE
      </button>
    </div>
    <div class="px-8 text-lg text-surface-600-400">
      There are no demo pipelines available at this time. Please refer to documentation for examples
      of SQL queries.
    </div>
  {/if}
</div>
