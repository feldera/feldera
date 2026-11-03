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
  import { tick } from 'svelte'
  import { enclosure, reclosureKey } from 'common-lib/function'
  import { type ExtendedPipeline } from '#lib/services/pipelineManager.ts'
  import Query, { type QueryData } from '#lib/components/adhoc/Query.svelte'
  import { isDataRow, type Row } from '#lib/types/adhocQuery.ts'
  import { isPipelineInteractive } from '#lib/functions/pipelines/status.ts'
  import { arrowIpcBatchToJS, arrowSchemaToFelderaFields } from '#lib/functions/apacheArrow.ts'
  import { type AsyncRecordBatchStreamReader, RecordBatchReader } from 'apache-arrow'
  import invariant from 'tiny-invariant'
  import WarningBanner from '#lib/components/pipelines/editor/WarningBanner.svelte'
  import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
  import { getSelectedTenant } from '#lib/services/auth.ts'

  let {
    pipeline,
    deleted = false
  }: { pipeline: { current: ExtendedPipeline }; deleted?: boolean } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let tenantName = $derived(getSelectedTenant() || '')
  let scrollEl: HTMLElement | undefined = $state()
  let isInteractive = $derived(!deleted && isPipelineInteractive(pipeline.current.status))

  $effect.pre(() => {
    if (!adhocQueries[tenantName]) {
      adhocQueries[tenantName] = {}
    }
    adhocQueries[tenantName][pipelineName] ??= { queries: [{ query: '' }] }
  })
  const api = usePipelineManager()

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
      let cancelled = false
      let arrowReader: AsyncRecordBatchStreamReader | null = null
      const stopReading = () => {
        cancelled = true
        arrowReader?.cancel().catch(() => {})
        result.cancel()
      }
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
            stopReading()
          })
        }
      }
      // Adhoc results are capped at `bufferSize` rows on the consumer side, so the
      // generic `parseStream` orchestrator (flush cadence + shedding budget) is
      // unnecessary here — drive the arrow reader directly.
      // Only the user's stop marks the query `stopped`: the row cap above also stops reading,
      // but leaves its own notice among the rows.
      adhocQueries[tenantName][pipelineName].queries[i].result.endResultStream = () => {
        const result = adhocQueries[tenantName][pipelineName].queries[i]?.result
        if (result) {
          result.stopped = true
        }
        stopReading()
      }
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
            // The new box appears above the query that just ran; scroll by the height it
            // adds, so that query and its results stay where they were on screen.
            const ranEl = scrollEl?.querySelector<HTMLElement>(`[data-query-index="${i}"]`)
            const topBefore = ranEl?.getBoundingClientRect().top
            adhocQueries[tenantName][pipelineName].queries.push({ query: '' })
            if (ranEl && scrollEl && topBefore !== undefined) {
              tick().then(() => {
                scrollEl!.scrollTop += ranEl.getBoundingClientRect().top - topBefore
              })
            }
          }
          adhocQueries[tenantName][pipelineName].queries[i].progress = false
        }
      }
    }
</script>

<!-- Newest query first: the box for the next query sits on top, earlier queries and their
     results follow. A size container, so results can be capped at a share of its height. -->
<div
  bind:this={scrollEl}
  class="[container-type:size] scrollbar h-full min-h-full overflow-y-auto px-3 pt-4"
>
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
    {#each [...(adhocQueries[tenantName]?.[pipelineName]?.queries ?? []).keys()].reverse() as i (i)}
      {@const x = adhocQueries[tenantName][pipelineName].queries[i]}
      {#if x}
        {invariant(adhocQueries[tenantName][pipelineName].queries[i])}
        <div data-query-index={i}>
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
            number={i + 1}
          ></Query>
        </div>
      {/if}
    {/each}
  </div>
</div>
