<script lang="ts" module>
  let adhocQueries = $state<
    Record<
      string,
      {
        queries: QueryData[]
      }
    >
  >({})
</script>

<script lang="ts">
  import { adHocQuery, type ExtendedPipeline } from '$lib/services/pipelineManager'
  import Query from '$lib/components/adhoc/Query.svelte'
  import { type QueryData } from '$lib/components/adhoc/Query.svelte'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let isIdle = $derived(isPipelineIdle(pipeline.current.status))

  $effect.pre(() => {
    adhocQueries[pipelineName] ??= { queries: [{ query: '' }] }
  })

  const onSubmitQuery = (i: number) => (query: string) => {
    if (adhocQueries[pipelineName].queries.length === i + 1) {
      adhocQueries[pipelineName].queries.push({ query: '' })
    }
    adhocQueries[pipelineName].queries[i].result = adHocQuery(pipelineName, query)
  }
</script>

<div class="flex min-h-full flex-col gap-4 p-2">
  {#if isIdle}
    <div class="sticky top-0 z-10 -m-2 mb-0 p-2 preset-tonal-warning">
      Start the pipeline to be able to execute queries
    </div>
  {/if}
  {#each adhocQueries[pipelineName].queries as _, i}
    <Query
      bind:value={adhocQueries[pipelineName].queries[i]}
      onSubmitQuery={onSubmitQuery(i)}
      onDeleteQuery={() => adhocQueries[pipelineName].queries.splice(i, 1)}
      disabled={isIdle}
      isLastQuery={adhocQueries[pipelineName].queries.length === i + 1}
    ></Query>
  {/each}
</div>
