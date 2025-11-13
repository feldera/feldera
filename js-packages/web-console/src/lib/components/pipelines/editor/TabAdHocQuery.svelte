<script lang="ts" module>
  let adhocQueries: Record<
    string,
    Record<
      string,
      {
        queries: (QueryData | undefined)[]
      }
    >
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
  import { getSelectedTenant } from '$lib/services/auth'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let tenantName = $derived(getSelectedTenant() || '')
  let isInteractive = $derived(isPipelineInteractive(pipeline.current.status))

  const reverseScroll = useReverseScrollContainer({
    observeContentElement: (e) => e.firstElementChild!
  })

  $effect.pre(() => {
    if (!adhocQueries[tenantName]) {
      adhocQueries[tenantName] = {}
    }
    adhocQueries[tenantName][pipelineName] ??= { queries: [{ query: '' }] }
  })
  const api = usePipelineManager()
  const isDataRow = (record: Record<string, SQLValueJS>) =>
    !(Object.keys(record).length === 1 && ('error' in record || 'warning' in record))

  const onSubmitQuery =
    (tenantName: string, pipelineName: string, i: number) => async (query: string) => {
      const request = api.adHocQuery(pipelineName, query)
      adhocQueries[tenantName][pipelineName].queries[i]!.progress = true
      const result = await request
      if (!adhocQueries[tenantName][pipelineName].queries[i]) {
        return
      }
      adhocQueries[tenantName][pipelineName].queries[i].result = {
        rows: enclosure([]),
        columns: [],
        totalSkippedBytes: 0,
        endResultStream: () => {}
      }
      if (result instanceof Error) {
        adhocQueries[tenantName][pipelineName].queries[i].progress = false
        adhocQueries[tenantName][pipelineName].queries[i].result.rows().push({
          error: result.message
        })
        return
      }
      const bufferSize = 1000
      const pushChanges = (
        input: (Record<string, SQLValueJS> | { error: string } | { warning: string })[]
      ) => {
        if (!adhocQueries[tenantName][pipelineName].queries[i]?.result) {
          return
        }
        if (
          adhocQueries[tenantName][pipelineName].queries[i].result.columns.length === 0 &&
          isDataRow(input[0])
        ) {
          adhocQueries[tenantName][pipelineName].queries[i].result.columns.push(
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
          const previousLength =
            adhocQueries[tenantName][pipelineName].queries[i].result.rows().length
          adhocQueries[tenantName][pipelineName].queries[i].result
            .rows()
            .push(
              ...input
                .slice(0, bufferSize - previousLength)
                .map((v) => (isDataRow(v) ? { cells: Object.values(v) } : v) as Row)
            )
          reclosureKey(adhocQueries[tenantName][pipelineName].queries[i].result, 'rows')
          if (input.length > bufferSize - previousLength) {
            queueMicrotask(() => {
              if (!adhocQueries[tenantName][pipelineName].queries[i]) {
                return
              }
              if (adhocQueries[tenantName][pipelineName].queries[i].result?.rows) {
                adhocQueries[tenantName][pipelineName].queries[i].result.rows().push({
                  warning: `The result contains more rows, but only the first ${bufferSize} are shown`
                })
                reclosureKey(adhocQueries[tenantName][pipelineName].queries[i].result, 'rows')
              }
              adhocQueries[tenantName][pipelineName].queries[i].result?.endResultStream()
            })
          }
        }
      }
      const { cancel } = parseCancellable(
        result,
        {
          pushChanges,
          onBytesSkipped: (skippedBytes) => {
            if (!adhocQueries[tenantName][pipelineName].queries[i]?.result) {
              return
            }
            adhocQueries[tenantName][pipelineName].queries[i].result.totalSkippedBytes +=
              skippedBytes
          },
          onParseEnded: () => {
            if (!adhocQueries[tenantName][pipelineName].queries[i]) {
              return
            }
            // Add field for the next query if the last query did not yield an error right away
            if (
              adhocQueries[tenantName][pipelineName].queries.length === i + 1 &&
              ((row) => !row || isDataRow(row))(
                adhocQueries[tenantName][pipelineName].queries[i].result?.rows().at(0)
              )
            ) {
              adhocQueries[tenantName][pipelineName].queries.push({ query: '' })
            }
            adhocQueries[tenantName][pipelineName].queries[i].progress = false
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
      adhocQueries[tenantName][pipelineName].queries[i].result.endResultStream = cancel
    }
</script>

<div class="h-full min-h-full overflow-y-auto scrollbar" use:reverseScroll.action>
  <div class="flex flex-col gap-6">
    {#if !isInteractive}
      <WarningBanner class="sticky top-0 z-20 -mb-4 -translate-y-2">
        Start the pipeline to be able to run queries
      </WarningBanner>
    {/if}
    {#each adhocQueries[tenantName]?.[pipelineName]?.queries ?? [] as x, i}
      {#if x}
        {invariant(adhocQueries[tenantName][pipelineName].queries[i])}
        <Query
          bind:query={adhocQueries[tenantName][pipelineName].queries[i].query}
          progress={adhocQueries[tenantName][pipelineName].queries[i]!.progress}
          result={adhocQueries[tenantName][pipelineName].queries[i]!.result}
          onSubmitQuery={onSubmitQuery(tenantName, pipelineName, i)}
          onDeleteQuery={() => {
            if (!adhocQueries[tenantName][pipelineName].queries[i]) {
              return
            }
            adhocQueries[tenantName][pipelineName].queries[i].result?.endResultStream()
            adhocQueries[tenantName][pipelineName].queries[i] = undefined // Overwrite with undefined instead of splice to preserve indices of other elements
          }}
          onCancelQuery={adhocQueries[tenantName][pipelineName].queries[i]!.progress
            ? adhocQueries[tenantName][pipelineName].queries[i].result?.endResultStream
            : undefined}
          disabled={!isInteractive}
          isLastQuery={adhocQueries[tenantName][pipelineName].queries.length === i + 1}
        ></Query>
      {/if}
    {/each}
  </div>
</div>
