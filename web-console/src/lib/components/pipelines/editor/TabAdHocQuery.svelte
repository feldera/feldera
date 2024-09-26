<script lang="ts" module>
  let adhocQueries: Record<
    string,
    {
      queries: QueryData[]
    }
  > = $state({})

  let getAdhocQueries = $state(() => adhocQueries)
</script>

<script lang="ts">
  import { adHocQuery, type ExtendedPipeline } from '$lib/services/pipelineManager'
  import Query, { type QueryResult, type Row } from '$lib/components/adhoc/Query.svelte'
  import { type QueryData } from '$lib/components/adhoc/Query.svelte'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'
  import type { Field } from '$lib/services/manager'
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import { parseUTF8JSON } from '$lib/functions/pipelines/changeStream'
  import invariant from 'tiny-invariant'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let isIdle = $derived(isPipelineIdle(pipeline.current.status))

  $effect.pre(() => {
    adhocQueries[pipelineName] ??= { queries: [{ query: '' }] }
  })

  $effect(() => {
    console.log('eeeeeeeeeee', JSON.stringify(adhocQueries[pipelineName].queries[0].progress))
  })

  const onSubmitQuery = (pipelineName: string, i: number) => (query: string) => {
    const promise = adHocQuery(pipelineName, query)
    adhocQueries[pipelineName].queries[i].progress = true
    promise.then((stream) => {
      adhocQueries[pipelineName].queries[i].result = {
        rows: [],
        columns: [],
        totalSkippedBytes: 0,
        endResultStream: () => {}
      }
      // let data: QueryResult = adhocQueries[pipelineName].queries[i].result!
      const bufferSize = 1000
      const pushChanges = (input: (Record<string, SQLValueJS> | { error: string })[]) => {
        if (!adhocQueries[pipelineName].queries[i].result) {
          return
        }
        // console.log('pushChanges', input)
        const isError = (record: Record<string, SQLValueJS>) =>
          Object.keys(record).length === 1 && 'error' in record
        if (
          adhocQueries[pipelineName].queries[i].result.columns.length === 0 &&
          !isError(input[0])
        ) {
          console.log('setting header', input[0])
          adhocQueries[pipelineName].queries[i].result.columns.push(
            ...Object.keys(input[0]).map((name) => ({
              name,
              case_sensitive: false,
              columntype: { nullable: true }
            }))
          )
        }
        adhocQueries[pipelineName].queries[i].result.rows.splice(
          0,
          adhocQueries[pipelineName].queries[i].result.rows.length + input.length - bufferSize
        )
        const x = input
          .slice(-bufferSize)
          .map((v) => (isError(v) ? v : { cells: Object.values(v) }) as Row)
        adhocQueries[pipelineName].queries[i].result.rows.push(...x)
        // console.log('x', x, adhocQueries[pipelineName].queries[i].result)
        getAdhocQueries = () => adhocQueries
        // adhocQueries[pipelineName].queries[i].result = data
      }
      const { cancel } = parseUTF8JSON(
        stream,
        pushChanges,
        (skippedBytes) => {
          if (!adhocQueries[pipelineName].queries[i].result) {
            return
          }
          adhocQueries[pipelineName].queries[i].result.totalSkippedBytes += skippedBytes
        },
        () => {
          adhocQueries[pipelineName].queries[i].progress = false
        },
        {
          paths: ['$'],
          bufferSize: 10 * 1024 * 1024
        }
      )
      adhocQueries[pipelineName].queries[i].result.endResultStream = cancel
    })
    promise.then(() => {
      // Add field for the next query if the last query was successful
      if (adhocQueries[pipelineName].queries.length === i + 1) {
        adhocQueries[pipelineName].queries.push({ query: '' })
      }
    })
  }
</script>

<div class="flex min-h-full flex-col gap-4 p-2">
  {#if isIdle}
    <div class="preset-tonal-warning sticky top-0 z-10 -m-2 mb-0 p-2">
      Start the pipeline to be able to execute queries
    </div>
  {/if}
  {#each adhocQueries[pipelineName].queries as x, i}
    <Query
      bind:query={adhocQueries[pipelineName].queries[i].query}
      progress={x.progress}
      result={adhocQueries[pipelineName].queries[i].result}
      onSubmitQuery={onSubmitQuery(pipelineName, i)}
      onDeleteQuery={() => {
        adhocQueries[pipelineName].queries[i].result?.endResultStream()
        adhocQueries[pipelineName].queries.splice(i, 1)
      }}
      disabled={isIdle}
      isLastQuery={adhocQueries[pipelineName].queries.length === i + 1}></Query>
  {/each}
</div>
