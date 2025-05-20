<script lang="ts" module>
  let adhocQueries: Record<
    string,
    {
      queries: (QueryData | undefined)[]
    }
  > = $state({})
</script>

<script lang="ts">
  import { type ExtendedPipeline } from '$lib/services/pipelineManager'
  import Query, { type Row, type QueryData } from '$lib/components/adhoc/Query.svelte'
  import { isPipelineInteractive } from '$lib/functions/pipelines/status'
  import type { SQLValueJS } from '$lib/types/sql.ts'
  import {
    CustomJSONParserTransformStream,
    parseCancellable
  } from '$lib/functions/pipelines/changeStream'
  import invariant from 'tiny-invariant'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { enclosure, reclosureKey } from '$lib/functions/common/function'
  import { useReverseScrollContainer } from '$lib/compositions/common/useReverseScrollContainer.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let isInteractive = $derived(isPipelineInteractive(pipeline.current.status))

  const reverseScroll = useReverseScrollContainer({
    observeContentElement: (e) => e.firstElementChild!
  })

  $effect.pre(() => {
    adhocQueries[pipelineName] ??= { queries: [{ query: '' }] }
  })
  const api = usePipelineManager()
  const isDataRow = (record: Record<string, SQLValueJS>) =>
    !(Object.keys(record).length === 1 && ('error' in record || 'warning' in record))

  const onSubmitQuery = (pipelineName: string, i: number) => async (query: string) => {
    const request = api.adHocQuery(pipelineName, query)
    adhocQueries[pipelineName].queries[i]!.progress = true
    const result = await request
    if (!adhocQueries[pipelineName].queries[i]) {
      return
    }
    adhocQueries[pipelineName].queries[i].result = {
      rows: enclosure([]),
      columns: [],
      totalSkippedBytes: 0,
      endResultStream: () => {}
    }
    if (result instanceof Error) {
      adhocQueries[pipelineName].queries[i].progress = false
      adhocQueries[pipelineName].queries[i].result.rows().push({
        error: result.message
      })
      return
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
            columntype: { nullable: true },
            unused: false
          }))
        )
      }
      {
        // Limit result size behavior - ignore all but first bufferSize rows
        const previousLength = adhocQueries[pipelineName].queries[i].result.rows().length
        adhocQueries[pipelineName].queries[i].result
          .rows()
          .push(
            ...input
              .slice(0, bufferSize - previousLength)
              .map((v) => (isDataRow(v) ? { cells: Object.values(v) } : v) as Row)
          )
        reclosureKey(adhocQueries[pipelineName].queries[i].result, 'rows')
        if (input.length > bufferSize - previousLength) {
          queueMicrotask(() => {
            if (!adhocQueries[pipelineName].queries[i]) {
              return
            }
            if (adhocQueries[pipelineName].queries[i].result?.rows) {
              adhocQueries[pipelineName].queries[i].result.rows().push({
                warning: `The result contains more rows, but only the first ${bufferSize} are shown`
              })
              reclosureKey(adhocQueries[pipelineName].queries[i].result, 'rows')
            }
            adhocQueries[pipelineName].queries[i].result?.endResultStream()
          })
        }
      }
    }
    const { cancel } = parseCancellable(
      result,
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
              adhocQueries[pipelineName].queries[i].result?.rows().at(0)
            )
          ) {
            adhocQueries[pipelineName].queries.push({ query: '' })
          }
          adhocQueries[pipelineName].queries[i].progress = false
        },
        onNetworkError(e, injectValue) {
          injectValue({ error: e.message })
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
  }
</script>

<div class=" h-full min-h-full overflow-y-auto py-2 scrollbar" use:reverseScroll.action>
  <div class="flex flex-col gap-6">
    {#if !isInteractive}
      <WarningBanner class="sticky top-0 z-20 -mb-4 -translate-y-2">
        Start the pipeline to be able to run queries
      </WarningBanner>
    {/if}
    {#each adhocQueries[pipelineName].queries as x, i}
      {#if x}
        {invariant(adhocQueries[pipelineName].queries[i])}
        <Query
          bind:query={adhocQueries[pipelineName].queries[i].query}
          progress={adhocQueries[pipelineName].queries[i]!.progress}
          result={adhocQueries[pipelineName].queries[i]!.result}
          onSubmitQuery={onSubmitQuery(pipelineName, i)}
          onDeleteQuery={() => {
            if (!adhocQueries[pipelineName].queries[i]) {
              return
            }
            adhocQueries[pipelineName].queries[i].result?.endResultStream()
            adhocQueries[pipelineName].queries[i] = undefined // Overwrite with undefined instead of splice to preserve indices of other elements
          }}
          onCancelQuery={adhocQueries[pipelineName].queries[i]!.progress
            ? adhocQueries[pipelineName].queries[i].result?.endResultStream
            : undefined}
          disabled={!isInteractive}
          isLastQuery={adhocQueries[pipelineName].queries.length === i + 1}
        ></Query>
      {/if}
    {/each}
  </div>
</div>
