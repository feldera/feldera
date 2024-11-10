<script lang="ts" module>
  let adhocQueries: Record<
    string,
    {
      queries: (QueryData | undefined)[]
    }
  > = {}

  let getAdhocQueries = $state(() => adhocQueries)
</script>

<script lang="ts">
  import { adHocQuery, type ExtendedPipeline } from '$lib/services/pipelineManager'
  import Query, { type Row } from '$lib/components/adhoc/Query.svelte'
  import { type QueryData } from '$lib/components/adhoc/Query.svelte'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import {
    CustomJSONParserTransformStream,
    parseCancellable
  } from '$lib/functions/pipelines/changeStream'
  import invariant from 'tiny-invariant'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let isIdle = $derived(isPipelineIdle(pipeline.current.status))

  $effect.pre(() => {
    adhocQueries[pipelineName] ??= { queries: [{ query: '' }] }
  })
  const isDataRow = (record: Record<string, SQLValueJS>) =>
    !(Object.keys(record).length === 1 && ('error' in record || 'warning' in record))

  const onSubmitQuery = (pipelineName: string, i: number) => (query: string) => {
    const promise = adHocQuery(pipelineName, query)
    adhocQueries[pipelineName].queries[i]!.progress = true
    getAdhocQueries = () => adhocQueries
    promise.then((stream) => {
      if (!adhocQueries[pipelineName].queries[i]) {
        return
      }
      adhocQueries[pipelineName].queries[i].result = {
        rows: [],
        columns: [],
        totalSkippedBytes: 0,
        endResultStream: () => {}
      }
      const bufferSize = 1000
      const pushChanges = (
        input: (Record<string, SQLValueJS> | { error: string } | { warning: string })[]
      ) => {
        if (!adhocQueries[pipelineName].queries[i]?.result) {
          return
        }
        if (
          adhocQueries[pipelineName].queries[i].result.columns.length === 0 &&
          isDataRow(input[0])
        ) {
          adhocQueries[pipelineName].queries[i].result.columns.push(
            ...Object.keys(input[0]).map((name) => ({
              name,
              case_sensitive: false,
              columntype: { nullable: true }
            }))
          )
        }
        {
          // Limit result size behavior - ignore all but first bufferSize rows
          const previousLength = adhocQueries[pipelineName].queries[i].result.rows.length
          adhocQueries[pipelineName].queries[i].result.rows.push(
            ...input
              .slice(0, bufferSize - previousLength)
              .map((v) => (isDataRow(v) ? { cells: Object.values(v) } : v) as Row)
          )
          getAdhocQueries = () => adhocQueries

          if (input.length > bufferSize - previousLength) {
            queueMicrotask(() => {
              if (!adhocQueries[pipelineName].queries[i]) {
                return
              }
              adhocQueries[pipelineName].queries[i].result?.rows.push({
                warning: `The result contains more rows, but only the first ${bufferSize} are shown`
              })
              getAdhocQueries = () => adhocQueries
              adhocQueries[pipelineName].queries[i].result?.endResultStream()
            })
          }
        }
      }
      const { cancel } = parseCancellable(
        stream,
        {
          pushChanges,
          onBytesSkipped: (skippedBytes) => {
            if (!adhocQueries[pipelineName].queries[i]?.result) {
              return
            }
            adhocQueries[pipelineName].queries[i].result.totalSkippedBytes += skippedBytes
          },
          onParseEnded: () => {
            if (!adhocQueries[pipelineName].queries[i]) {
              return
            }
            // Add field for the next query if the last query did not yield an error right away
            if (
              adhocQueries[pipelineName].queries.length === i + 1 &&
              ((row) => !row || isDataRow(row))(
                adhocQueries[pipelineName].queries[i].result?.rows.at(0)
              )
            ) {
              adhocQueries[pipelineName].queries.push({ query: '' })
            }
            adhocQueries[pipelineName].queries[i].progress = false
            getAdhocQueries = () => adhocQueries
          }
        },
        new CustomJSONParserTransformStream<Record<string, SQLValueJS>>({
          paths: ['$'],
          separator: ''
        }),
        {
          bufferSize: 8 * 1024 * 1024
        }
      )
      adhocQueries[pipelineName].queries[i].result.endResultStream = cancel
    })
  }
</script>

<div class="bg-white-black flex h-full min-h-full flex-col gap-6 overflow-y-auto p-2 scrollbar">
  {#if isIdle}
    <WarningBanner class="sticky top-0 z-10 -mx-2 -mb-4 -translate-y-2">
      Start the pipeline to be able to execute queries
    </WarningBanner>
  {/if}
  {#each getAdhocQueries()[pipelineName].queries as x, i}
    {#if x}
      {invariant(adhocQueries[pipelineName].queries[i])}
      <Query
        bind:query={adhocQueries[pipelineName].queries[i].query}
        progress={getAdhocQueries()[pipelineName].queries[i]!.progress}
        result={getAdhocQueries()[pipelineName].queries[i]!.result}
        onSubmitQuery={onSubmitQuery(pipelineName, i)}
        onDeleteQuery={() => {
          if (!adhocQueries[pipelineName].queries[i]) {
            return
          }
          adhocQueries[pipelineName].queries[i].result?.endResultStream()
          delete adhocQueries[pipelineName].queries[i] // Delete instead of splice to preserve indices of other elements
          getAdhocQueries = () => adhocQueries
        }}
        onCancelQuery={getAdhocQueries()[pipelineName].queries[i]!.progress
          ? adhocQueries[pipelineName].queries[i].result?.endResultStream
          : undefined}
        disabled={isIdle}
        isLastQuery={getAdhocQueries()[pipelineName].queries.length === i + 1}
      ></Query>
    {/if}
  {/each}
</div>
