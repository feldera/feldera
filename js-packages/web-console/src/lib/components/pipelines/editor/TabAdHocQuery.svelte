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
  import Query, { type QueryData, type QueryResult } from '#lib/components/adhoc/Query.svelte'
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
  // Queries are keyed by the id, so a rename keeps them
  let pipelineId = $derived(pipeline.current.id)
  let tenantName = $derived(getSelectedTenant() || '')
  let scrollEl: HTMLElement | undefined = $state()
  let isInteractive = $derived(!deleted && isPipelineInteractive(pipeline.current.status))

  $effect.pre(() => {
    if (!adhocQueries[tenantName]) {
      adhocQueries[tenantName] = {}
    }
    adhocQueries[tenantName][pipelineId] ??= { queries: [{ query: '' }] }
  })
  const api = usePipelineManager()

  const queries = $derived(adhocQueries[tenantName]?.[pipelineId]?.queries)

  const onSubmitQuery = (i: number) => async (sql: string) => {
    // Rows that arrive after the user switches to another pipeline go to the submitting pipeline.
    // The code never replaces this array, so the reference stays valid.
    const submittedQueries = adhocQueries[tenantName][pipelineId].queries
    // The user can delete the query at any time. An update applies only while the query
    // exists, and returns whether the query exists.
    const updateQuery = (update: (query: QueryData) => void) => {
      const query = submittedQueries[i]
      if (query) {
        update(query)
      }
      return query !== undefined
    }
    const updateResult = (update: (result: QueryResult) => void) =>
      updateQuery((query) => {
        if (query.result) {
          update(query.result)
        }
      })

    const request = api.adHocQuery(pipeline.current.name, sql)
    updateQuery((query) => {
      query.progress = true
    })
    const result = await request
    const isQueryKept = updateQuery((query) => {
      query.result = {
        rows: enclosure([]),
        columns: [],
        totalSkippedBytes: 0,
        endResultStream: () => {}
      }
    })
    if (!isQueryKept) {
      return
    }
    if (result instanceof Error) {
      updateQuery((query) => {
        query.progress = false
        query.result?.rows().push({ error: result.message })
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
    const appendRows = (rows: Row[]) =>
      updateResult((queryResult) => {
        // Keep only the first `bufferSize` rows
        const previousLength = queryResult.rows().length
        queryResult.rows().push(...rows.slice(0, bufferSize - previousLength))
        reclosureKey(queryResult, 'rows')
        if (rows.length <= bufferSize - previousLength) {
          return
        }
        queueMicrotask(() => {
          const isQueryKept = updateResult((queryResult) => {
            queryResult.rows().push({
              warning: `The result contains more rows, but only the first ${bufferSize} are shown`
            })
            reclosureKey(queryResult, 'rows')
          })
          if (isQueryKept) {
            stopReading()
          }
        })
      })
    // Ad-hoc results are capped at `bufferSize` rows on the consumer side, so this code reads
    // the arrow stream directly. It does not need the flush cadence and shedding budget of
    // the generic `parseStream` orchestrator.
    // Only the user's stop marks the query `stopped`. The row cap above also stops reading,
    // but adds its own warning row.
    updateResult((queryResult) => {
      queryResult.endResultStream = () => {
        updateResult((queryResult) => {
          queryResult.stopped = true
        })
        stopReading()
      }
    })
    try {
      arrowReader = (await RecordBatchReader.from(
        result.stream
      )) as unknown as AsyncRecordBatchStreamReader
      // For the async variant, `RecordBatchReader.from` returns before it parses the stream
      // header. `schema` is set only after `open()`.
      await arrowReader.open()
      if (cancelled) {
        return
      }
      // A query that fails before it produces a schema has no arrow schema.
      // `adHocQuery` reports the error out of band.
      const schema = arrowReader.schema
      if (schema) {
        updateResult((queryResult) => {
          queryResult.columns.push(...arrowSchemaToFelderaFields(schema))
        })
      }
      for await (const batch of arrowReader) {
        if (cancelled) {
          return
        }
        const rows: Row[] = arrowIpcBatchToJS(batch).map(({ row }) => ({ cells: row }) as Row)
        appendRows(rows)
      }
      // The arrow stream always closes cleanly, so `result.error` gives a query error that
      // occurs up front or mid-stream. Throw it, so the catch below renders it.
      const queryError = result.error?.()
      if (queryError) {
        throw queryError
      }
    } catch (e) {
      if (!cancelled) {
        appendRows([{ error: e instanceof Error ? e.message : String(e) }])
      }
    } finally {
      updateQuery((query) => {
        // Add field for the next query if the last query did not yield an error right away
        const isLastQuery = submittedQueries.length === i + 1
        const firstRow = query.result?.rows().at(0)
        if (isLastQuery && (!firstRow || isDataRow(firstRow))) {
          // The new box appears above the query that just ran. Scroll by the height of the
          // new box, so that query and its results stay where they were on screen.
          const ranEl = scrollEl?.querySelector<HTMLElement>(`[data-query-index="${i}"]`)
          const topBefore = ranEl?.getBoundingClientRect().top
          submittedQueries.push({ query: '' })
          if (ranEl && scrollEl && topBefore !== undefined) {
            tick().then(() => {
              scrollEl!.scrollTop += ranEl.getBoundingClientRect().top - topBefore
            })
          }
        }
        query.progress = false
      })
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
    {#if queries}
      {#each [...queries.keys()].reverse() as i (i)}
        {@const x = queries[i]}
        {#if x}
          {invariant(queries[i])}
          <div data-query-index={i}>
            <Query
              bind:query={queries[i].query}
              progress={queries[i].progress}
              result={queries[i].result}
              onSubmitQuery={onSubmitQuery(i)}
              onDeleteQuery={() => {
                if (!queries[i]) {
                  return
                }
                queries[i].result?.endResultStream()
                queries[i] = undefined // Overwrite with undefined instead of splice to preserve indices of other elements
              }}
              onCancelQuery={queries[i].progress ? queries[i].result?.endResultStream : undefined}
              disabled={!isInteractive}
              isLastQuery={queries.length === i + 1}
              number={i + 1}
            ></Query>
          </div>
        {/if}
      {/each}
    {/if}
  </div>
</div>
