<script lang="ts">
  import { goto } from '$app/navigation'
  import { useTryPipeline } from '$lib/compositions/pipelines/useTryPipeline'

  let { data } = $props()
  const tryPipeline = useTryPipeline()
</script>

<div class="self-center">
  {#if data.demos.length}
    <div class="h5 px-8 py-8 font-normal">Try running one of our examples below.</div>
    <div
      class="grid max-w-[1390px] grid-cols-1 gap-8 px-8 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4"
    >
      {#each data.demos as demo}
        <div class="card flex flex-col gap-2 bg-white p-4 dark:bg-black">
          <button class="text-left text-primary-500" onclick={() => tryPipeline(demo.pipeline)}>
            <span class="text-lg">{demo.title}</span>
            <span class="fd fd-arrow_forward w-0 translate-y-0.5 scale-150"></span>
          </button>
          <span class="text-left">{demo.pipeline.description}</span>
        </div>
      {/each}
    </div>
  {:else}
    <div class="h5 px-8 py-8 font-normal">
      Write a new SQL query from scratch:
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
