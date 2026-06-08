<script lang="ts" module>
  const adhocQueries: Record<
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
  import { arrowIpcBatchToJS, arrowSchemaToFelderaFields } from '$lib/functions/apacheArrow'
  import { type AsyncRecordBatchStreamReader, RecordBatchReader } from 'apache-arrow'
  import invariant from 'tiny-invariant'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { enclosure, reclosureKey } from '$lib/functions/common/function'
  import { useReverseScrollContainer } from 'common-ui'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { getSelectedTenant } from '$lib/services/auth'

  let {
    pipeline,
    deleted = false
  }: { pipeline: { current: ExtendedPipeline }; deleted?: boolean } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let tenantName = $derived(getSelectedTenant() || '')
  let isInteractive = $derived(!deleted && isPipelineInteractive(pipeline.current.status))

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

  const isDataRow = (row: Row): row is { cells: unknown[] } & Row => 'cells' in row

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
      const appendRows = (rows: Row[]) => {
        if (!adhocQueries[tenantName][pipelineName].queries[i]?.result) {
          return
        }
        // Limit result size - ignore all but first bufferSize rows
        const previousLength =
          adhocQueries[tenantName][pipelineName].queries[i].result.rows().length
        adhocQueries[tenantName][pipelineName].queries[i].result
          .rows()
          .push(...rows.slice(0, bufferSize - previousLength))
        reclosureKey(adhocQueries[tenantName][pipelineName].queries[i].result, 'rows')
        if (rows.length > bufferSize - previousLength) {
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
      // Adhoc results are capped at `bufferSize` rows on the consumer side, so the
      // generic `parseStream` orchestrator (flush cadence + shedding budget) is
      // unnecessary here — drive the arrow reader directly.
      let cancelled = false
      let arrowReader: AsyncRecordBatchStreamReader | null = null
      const cancel = () => {
        cancelled = true
        arrowReader?.cancel().catch(() => {})
        result.cancel()
      }
      adhocQueries[tenantName][pipelineName].queries[i].result.endResultStream = cancel
      try {
        arrowReader = (await RecordBatchReader.from(
          result.stream
        )) as unknown as AsyncRecordBatchStreamReader
        // `RecordBatchReader.from` returns before the stream header has been parsed
        // for the async variant — `schema` is only populated after `open()`.
        await arrowReader.open()
        if (cancelled) {
          return
        }
        // A query that fails before producing a schema yields no arrow schema;
        // the actual error is reported out of band (see `adHocQuery`).
        if (arrowReader.schema && adhocQueries[tenantName][pipelineName].queries[i]?.result) {
          adhocQueries[tenantName][pipelineName].queries[i].result.columns.push(
            ...arrowSchemaToFelderaFields(arrowReader.schema)
          )
        }
        for await (const batch of arrowReader) {
          if (cancelled) {
            return
          }
          const rows: Row[] = arrowIpcBatchToJS(batch).map(({ row }) => ({ cells: row }) as Row)
          appendRows(rows)
        }
        // The arrow stream is always closed cleanly; a query error (up front or
        // mid-stream) surfaces here so the catch below renders it.
        const queryError = result.error?.()
        if (queryError) {
          throw queryError
        }
      } catch (e) {
        if (!cancelled) {
          appendRows([{ error: e instanceof Error ? e.message : String(e) }])
        }
      } finally {
        if (adhocQueries[tenantName][pipelineName]?.queries[i]) {
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
        }
      }
    }
</script>

<div class="scrollbar h-full min-h-full overflow-y-auto" use:reverseScroll.action>
  <div class="flex flex-col gap-6">
    {#if deleted}
      <div data-testid="box-adhoc-banner">
        <WarningBanner class="sticky top-0 z-20 -mb-2" variant="info">
          Queries are disabled. The pipeline has been deleted.
        </WarningBanner>
      </div>
    {:else if !isInteractive}
      <div data-testid="box-adhoc-banner">
        <WarningBanner class="sticky top-0 z-20 -mb-2">
          Start the pipeline to be able to run queries
        </WarningBanner>
      </div>
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
