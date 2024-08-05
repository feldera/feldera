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
    <div class="h5 px-8 py-8 font-normal md:px-16">
      Try running one of our examples below, or write a new pipeline from scratch:
      <button
        class="btn preset-filled-primary-500 mt-auto self-end text-sm"
        onclick={() => goto('#new')}>
        CREATE NEW PIPELINE
      </button>
    </div>
    <div
      class="grid max-w-[1400px] grid-cols-1 gap-8 px-8 sm:grid-cols-2 md:gap-16 md:px-16 lg:grid-cols-3 xl:grid-cols-4">
      {#each data.demos as demo}
        <div class="card flex flex-col gap-2 bg-white p-4 dark:bg-black">
          <span class="h5 font-normal">{demo.title}</span>
          <span class="text-left">{demo.pipeline.description}</span>
          <button
            onclick={() => tryPipelineFromExample(demo.pipeline)}
            class="btn preset-filled-primary-500 mt-auto self-end text-sm">
            TRY
            <div class="bx bx-right-arrow-alt text-[24px]"></div>
          </button>
        </div>
      {/each}
    </div>
  {:else}
    <div class="h5 px-8 py-8 font-normal md:px-16">
      Write a new streaming SQL query from scratch:
      <button
        class="btn preset-filled-primary-500 mt-auto self-end text-sm"
        onclick={() => goto('#new')}>
        CREATE NEW PIPELINE
      </button>
    </div>
    <div class="text-surface-600-400 px-8 text-lg md:px-16">
      There are no demo pipelines available at this time. Please refer to documentation for examples
      of SQL queries.
    </div>
  {/if}
</div>
