<script lang="ts" module>
  /**
   * TabChangeStream - Real-time pipeline change stream viewer
   *
   * STREAM LIFETIME MANAGEMENT:
   *
   * This component manages HTTP streams for viewing pipeline data changes in real-time.
   * The stream lifetime is limited to pipeline edit page being opened.
   * Due to the component being kept alive (keepAlive: true in InteractionsPanel), special care
   * is needed to manage stream lifecycles properly.
   *
   * Stream Start Conditions:
   * 1. When a relation (table/view) checkbox is checked by the user
   * 2. When the pipeline transitions to 'start_paused' state (via pipelineActionCallbacks)
   * 3. When pipelineId/tenantName changes AND the pipeline is interactive (Running/Paused)
   *
   * Stream Stop/Cleanup Conditions:
   * 1. When a relation checkbox is unchecked by the user
   * 2. When pipelineId or tenantName changes (cleanup callback cancels old pipeline streams)
   * 3. When pipeline is deleted (via 'delete' action callback)
   *
   * Pipeline Interactive States (can stream data):
   * - Running, Paused, Pausing, Resuming
   *
   * Key Implementation Details:
   * - startSelectedStreams(): Starts streams for all selected relations, checks isPipelineInteractive
   * - Effect on pipelineId/tenantName: Non-reactive to isInteractive, only starts if interactive
   * - 'start_paused' callback: Always attempts to start streams (pipeline state guarantees validity)
   * - Each stream has a cancelStream() function stored in pipelinesRelations for cleanup
   * - Buffer clearing happens in startSelectedStreams to reset data when pipeline restarts
   * - State is keyed by pipeline id, so a rename keeps the selected relations and the rows.
   *   `pipelineNames` maps the id to the current name, which the API and the action
   *   callbacks use.
   */

  import type { ChangeStreamData, Row } from '#lib/components/pipelines/editor/ChangeStream.svelte'

  type RelationInfo = {
    pipelineName: string
    relationName: string
  }
  type ExtraType = {
    fields: Record<string, Field>
    selected: boolean
    // Full teardown: stops the network read AND drops this relation's rows from the
    // buffer (used when a relation is unchecked or the pipeline is torn down).
    cancelStream?: () => void
    // Bare network stop used by scroll-pause: halts reading but keeps the rows already
    // received so the paused view stays put. Resume re-establishes the stream.
    stopStream?: () => void
  }

  // Tenant -> pipeline id -> current pipeline name
  const pipelineNames: Record<string, Record<string, string>> = {}
  const findPipelineId = (tenantName: string, pipelineName: string) =>
    Object.entries(pipelineNames[tenantName] ?? {}).find(([, name]) => name === pipelineName)?.[0]
  // Tenant -> pipeline id -> the 'start_paused' callback and the name it is registered under
  const startPausedCallbacks: Record<
    string,
    Record<string, { pipelineName: string; callback: () => Promise<void> }>
  > = {}

  let pipelinesRelations = $state<
    Record<string, Record<string, Record<string, ExtraType & { type: 'tables' | 'views' }>>>
  >({})
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const changeStream: Record<string, Record<string, ChangeStreamData>> = {} // Initialize row array nested by tenant and pipeline
  // Separate getRows as a $state avoids burdening rows array itself with reactivity overhead
  const getChangeStream = new Ref(changeStream)

  const bufferSize = 10000
  // Drop the rows of one relation, and keep the rows of the others
  const dropRelationRows = (tenantName: string, pipelineId: string, relationName: string) => {
    const pipelineChangeStream = changeStream[tenantName]?.[pipelineId]
    if (!pipelineChangeStream) {
      return
    }
    ;({ rows: pipelineChangeStream.rows, headers: pipelineChangeStream.headers } = filterOutRows(
      pipelineChangeStream.rows,
      pipelineChangeStream.headers,
      relationName
    ))
  }
  const filterOutRows = (rows: Row[], headers: number[], relationName: string) => {
    let batchRelationName: string | undefined
    const newRows = rows.filter((row) => {
      if ('skippedBytes' in row && batchRelationName === undefined) {
        return true
      }
      if ('relationName' in row) {
        batchRelationName = row.relationName
      }
      return batchRelationName !== relationName
    })
    const newHeaders = (() => {
      const res: number[] = []
      for (let i = 0; i < newRows.length; ++i) {
        if ('relationName' in newRows[i]) {
          res.push(i)
        }
      }
      return res
    })()
    return {
      rows: newRows,
      headers: newHeaders
    }
  }
  const startReadingStream = (
    api: PipelineManagerApi,
    tenantName: string,
    pipelineId: string,
    relationName: string
  ) => {
    const clearStreamHandles = () => {
      const relation = pipelinesRelations[tenantName]?.[pipelineId]?.[relationName]
      if (relation) {
        relation.cancelStream = undefined
        relation.stopStream = undefined
      }
    }
    const pipelineName = pipelineNames[tenantName][pipelineId]
    const request = api.relationEgressStream(pipelineName, relationName).then((result) => {
      if (result instanceof Error) {
        clearStreamHandles()
        return undefined
      }

      // Build a section header for `relationName`. Prefers a sample data row to
      // determine column order; falls back to the relation schema (e.g. for the
      // skipped-bytes path, where there's no row to sample).
      const buildSectionHeader = (sample?: XgressEntry): Row => {
        const fields = pipelinesRelations[tenantName][pipelineId][relationName].fields
        const columns: Field[] = sample
          ? Object.keys('insert' in sample ? sample.insert : sample.delete).map(
              (name) => fields[normalizeCaseIndependentName({ name })]
            )
          : Object.values(fields)
        return { relationName, columns }
      }

      const appendForRelation = (newRows: Row[], sample?: XgressEntry) =>
        appendRowsForRelation(
          changeStream[tenantName][pipelineId],
          relationName,
          newRows,
          buildSectionHeader,
          bufferSize,
          sample
        )

      const { cancel } = parseStream(
        result,
        newlineJsonDecoder<XgressEntry>(
          createBigNumberStreamParser<XgressEntry>({
            paths: ['$.json_data.*'],
            separator: ''
          }),
          {
            bufferSize: 4 * 1024 * 1024,
            onSkipped: ({ bytes: skippedBytes }) => {
              // The delete callback can drop the history while the stream still delivers rows
              const cs = changeStream[tenantName]?.[pipelineId]
              if (!cs) {
                return
              }
              // Coalesce consecutive skip markers for the same relation: if the row
              // at the tail is already a skip marker tagged with this relation, just
              // bump its byte count in place instead of pushing another row. Keeps
              // the change-stream view from getting flooded with one-line "Skipped N bytes"
              // entries when backpressure drops sustained traffic.
              const lastRow = cs.rows.at(-1)
              if (lastRow && 'skippedBytes' in lastRow && lastRow.relationName === relationName) {
                lastRow.skippedBytes += skippedBytes
              } else {
                appendForRelation([{ relationName, skippedBytes }])
              }
              cs.totalSkippedBytes += skippedBytes
            }
          }
        ),
        {
          pushChanges: (rows: XgressEntry[]) => {
            if (!changeStream[tenantName]?.[pipelineId]) {
              return
            }
            appendForRelation(rows as unknown as Row[], rows[0])
          },
          onParseEnded: () => {
            clearStreamHandles()
          }
        }
      )
      return () => {
        cancel()
      }
    })
    // Bare network stop: halt reading without removing already-received rows. Drives
    // scroll-pause, where the paused view must keep showing the latest data.
    pipelinesRelations[tenantName][pipelineId][relationName].stopStream = () => {
      request.then((cancel) => {
        cancel?.()
        clearStreamHandles()
      })
    }
    // Full teardown: stop reading and drop this relation's rows from the buffer (used when a
    // relation is unchecked or the pipeline is torn down).
    pipelinesRelations[tenantName][pipelineId][relationName].cancelStream = () => {
      request.then((cancel) => {
        cancel?.()
        clearStreamHandles()
        dropRelationRows(tenantName, pipelineId, relationName)
        getChangeStream.current = changeStream
      })
    }
  }
  const startSelectedStreams = (
    api: PipelineManagerApi,
    tenantName: string,
    pipelineId: string
  ) => {
    changeStream[tenantName][pipelineId] = { rows: [], headers: [], totalSkippedBytes: 0 } // Clear row buffer when starting pipeline again
    const relations = Object.entries(pipelinesRelations[tenantName]?.[pipelineId] ?? {})
      .filter((relation) => relation[1].selected)
      .map((relation) => relation[0])
    for (const relationName of relations) {
      if (pipelinesRelations[tenantName][pipelineId][relationName].cancelStream) {
        continue
      }
      startReadingStream(api, tenantName, pipelineId, relationName)
    }
    getChangeStream.current = changeStream
  }
  // Scroll-pause: stop reading every selected relation's stream without touching the
  // buffer, so the view freezes on the rows already received and no new data arrives.
  const pauseSelectedStreams = (tenantName: string, pipelineId: string) => {
    const relations = pipelinesRelations[tenantName]?.[pipelineId]
    if (!relations) {
      return
    }
    for (const relation of Object.values(relations)) {
      if (relation.selected) {
        relation.stopStream?.()
      }
    }
  }
  // Scroll-resume: re-establish a stream for every selected relation that isn't already
  // streaming. Unlike `startSelectedStreams`, the existing buffer is preserved — resuming
  // continues the history rather than wiping it.
  const resumeSelectedStreams = (
    api: PipelineManagerApi,
    tenantName: string,
    pipelineId: string
  ) => {
    const relations = pipelinesRelations[tenantName]?.[pipelineId]
    if (!relations) {
      return
    }
    for (const [relationName, relation] of Object.entries(relations)) {
      if (!relation.selected || relation.cancelStream) {
        continue
      }
      startReadingStream(api, tenantName, pipelineId, relationName)
    }
    getChangeStream.current = changeStream
  }
  const registerPipeline = (
    api: PipelineManagerApi,
    tenantName: string,
    pipelineId: string,
    pipelineName: string
  ) => {
    pipelineNames[tenantName] ??= {}
    pipelineNames[tenantName][pipelineId] = pipelineName
    // Action callbacks are keyed by name, so move the callback when the pipeline is renamed
    startPausedCallbacks[tenantName] ??= {}
    const registered = startPausedCallbacks[tenantName][pipelineId]
    if (registered?.pipelineName !== pipelineName) {
      if (registered) {
        pipelineActionCallbacks.remove(registered.pipelineName, 'start_paused', registered.callback)
      }
      const callback = async () => {
        startSelectedStreams(api, tenantName, pipelineId)
      }
      pipelineActionCallbacks.add(pipelineName, 'start_paused', callback)
      startPausedCallbacks[tenantName][pipelineId] = { pipelineName, callback }
    }
    if (!pipelinesRelations[tenantName]) {
      pipelinesRelations[tenantName] = {}
    }
    if (pipelinesRelations[tenantName][pipelineId]) {
      return
    }
    pipelinesRelations[tenantName][pipelineId] = {}
    if (!changeStream[tenantName]) {
      changeStream[tenantName] = {}
    }
    changeStream[tenantName][pipelineId] = { rows: [], headers: [], totalSkippedBytes: 0 }
  }
  // Delete callbacks get the name of the deleted pipeline
  const dropChangeStreamHistory = async (tenantName: string, pipelineName: string) => {
    const pipelineId = findPipelineId(tenantName, pipelineName)
    if (pipelineId === undefined) {
      return
    }
    if (pipelinesRelations[tenantName]) {
      delete pipelinesRelations[tenantName][pipelineId]
    }
    if (changeStream[tenantName]) {
      delete changeStream[tenantName][pipelineId]
    }
  }

  /**
   * Check if a pipeline has any selected relations in the Change Stream tab.
   * This is used to optimize pipeline start behavior - if no relations are selected,
   * we can skip the pause-unpause sequence.
   */
  export const hasSelectedRelations = (pipelineName: string): boolean => {
    const tenantName = getSelectedTenant() || ''
    const pipelineId = findPipelineId(tenantName, pipelineName)
    if (pipelineId === undefined) {
      return false
    }
    const relations = pipelinesRelations[tenantName]?.[pipelineId]
    if (!relations) {
      return false
    }
    return Object.values(relations).some((r) => r.selected)
  }
</script>

<script lang="ts">
  import { count, groupBy } from 'common-lib/array'
  import { getCaseIndependentName, normalizeCaseIndependentName } from 'common-ui/felderaRelation'
  import { tuple } from 'common-lib/tuple'
  import { usePipelineActionCallbacks } from '#lib/compositions/pipelines/usePipelineActionCallbacks.svelte.ts'

  import { type ExtendedPipeline, type XgressEntry } from '#lib/services/pipelineManager.ts'
  import ChangeStream from './ChangeStream.svelte'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import type { Field, Relation } from '#lib/services/manager/index.ts'
  import {
    appendRowsForRelation,
    createBigNumberStreamParser,
    newlineJsonDecoder,
    parseStream
  } from '#lib/functions/pipelines/changeStream.ts'
  import JSONbig from 'true-json-bigint'
  import { untrack } from 'svelte'
  import { SegmentedControl, Tooltip, useIsMobile } from 'common-ui'
  import {
    usePipelineManager,
    type PipelineManagerApi
  } from '#lib/compositions/usePipelineManager.svelte.ts'
  import { useProtocol } from '#lib/compositions/useProtocol.ts'
  import { getSelectedTenant } from '#lib/services/auth.ts'
  import { isPipelineInteractive } from '#lib/functions/pipelines/status.ts'
  import { Ref } from '#lib/compositions/ref.svelte.ts'
  import WarningBanner from '#lib/components/pipelines/editor/WarningBanner.svelte'
  import { useDarkMode } from '#lib/compositions/useDarkMode.svelte.ts'
  import ChangeStreamLight from '#assets/illustrations/change-stream-light.svg?component'
  import ChangeStreamDark from '#assets/illustrations/change-stream-dark.svg?component'

  let {
    pipeline,
    deleted = false
  }: { pipeline: { current: ExtendedPipeline }; deleted?: boolean } = $props()

  const api = usePipelineManager()
  let pipelineId = $derived(pipeline.current.id)
  let pipelineName = $derived(pipeline.current.name)
  let tenantName = $derived(getSelectedTenant() || '')
  let isInteractive = $derived(!deleted && isPipelineInteractive(pipeline.current.status))

  const protocol = useProtocol()
  const maxStreamsOnHttp = 4

  let selectedRelationsCount = $derived(
    count(Object.values(pipelinesRelations), (tenantRelations) =>
      count(Object.values(tenantRelations), (relations) =>
        count(Object.values(relations), (r) => r.selected)
      )
    )
  )

  const reloadSchema = async (
    tenantName: string,
    pipelineId: string,
    pipeline: ExtendedPipeline
  ) => {
    const schema = pipeline.programInfo?.schema
    if (!schema) {
      return
    }
    const oldSchema = pipelinesRelations[tenantName]?.[pipelineId]
    if (!pipelinesRelations[tenantName]) {
      pipelinesRelations[tenantName] = {}
    }
    pipelinesRelations[tenantName][pipelineId] = {}
    const process = (type: 'tables' | 'views', newRelations: Relation[]) => {
      for (const newRelation of newRelations) {
        const newRelationName = getCaseIndependentName(newRelation)
        const oldRelation =
          oldSchema?.[newRelationName]?.type === type && oldSchema[newRelationName]
        pipelinesRelations[tenantName][pipelineId][newRelationName] = oldRelation || {
          type,
          selected: false,
          fields: Object.fromEntries(
            newRelation.fields.map((f) => tuple(getCaseIndependentName(f), f))
          )
        }
      }
    }
    process('tables', schema.inputs)
    process('views', schema.outputs)
  }

  $effect.pre(() => {
    registerPipeline(api, tenantName, pipelineId, pipelineName)
  })

  $effect(() => {
    void pipeline.current
    untrack(() => reloadSchema(tenantName, pipelineId, pipeline.current))
  })

  $effect(() => {
    pipelineId
    tenantName

    const oldPipelineId = pipelineId

    // Start streams for any selected relations when pipeline is interactive
    untrack(() => {
      if (isInteractive) {
        startSelectedStreams(api, tenantName, pipelineId)
      }
    })

    return () => {
      // Clean up active streams when pipelineId changes
      const relations = pipelinesRelations[tenantName]?.[oldPipelineId]
      if (relations) {
        for (const relation of Object.values(relations)) {
          relation.cancelStream?.()
        }
      }
    }
  })

  let inputs = $derived(
    Object.entries(pipelinesRelations[tenantName]?.[pipelineId] ?? {})
      .filter((e) => e[1].type === 'tables')
      .map(([relationName, value]) => ({
        relationName,
        ...value
      }))
  )
  let outputs = $derived(
    Object.entries(pipelinesRelations[tenantName]?.[pipelineId] ?? {})
      .filter((e) => e[1].type === 'views')
      .map(([relationName, value]) => ({
        relationName,
        ...value
      }))
  )

  const visualUpdateMs = 100
  // Update visible list of changes at a constant time period
  $effect(() => {
    const update = () => {
      getChangeStream.current = changeStream
    }
    const handle = setInterval(update, visualUpdateMs)
    update()
    return () => {
      clearInterval(handle)
    }
  })

  $effect(() => {
    const dropCallback = async (pName: string) => {
      await dropChangeStreamHistory(tenantName, pName)
    }
    untrack(() => pipelineActionCallbacks.add('', 'delete', dropCallback))
    return () => {
      pipelineActionCallbacks.remove('', 'delete', dropCallback)
    }
  })

  const pasteChanges = (changes: { relationName: string; values: XgressEntry[] }[]) => {
    for (const batch of changes) {
      api.relationIngress(pipelineName, batch.relationName, batch.values, 'force')
    }
  }
  const ingestPasted = (e: ClipboardEvent) => {
    e.preventDefault()
    let pastedData = JSONbig.parse(e.clipboardData!.getData('text/plain'))
    if (!Array.isArray(pastedData)) {
      pastedData = [pastedData]
    }
    pasteChanges(
      groupBy(
        pastedData as ({ relationName: string } & XgressEntry)[],
        (row) => row.relationName
      ).map(([relationName, values]) => ({
        relationName,
        values: values.map(({ relationName, ...v }) => v)
      }))
    )
  }

  const isMobile = useIsMobile()
  const darkMode = useDarkMode()
  const mobileDisplayModes = ['Tables and Views', 'Data stream'] as const
  type MobileDisplayMode = (typeof mobileDisplayModes)[number]
  let mobileDisplayMode = $state<MobileDisplayMode>('Tables and Views')
  const mobileDisplayItems: { value: MobileDisplayMode; label: string }[] = mobileDisplayModes.map(
    (value) => ({ value, label: value })
  )
</script>

{#snippet relationView()}
  {#snippet relationItem(relation: RelationInfo & ExtraType)}
    {@const isDisabled =
      protocol === 'http' && !relation.selected && selectedRelationsCount >= maxStreamsOnHttp}
    <label
      class="flex flex-none! items-center gap-1 overflow-hidden {isDisabled
        ? 'cursor-not-allowed opacity-50'
        : 'cursor-pointer'}"
    >
      <input
        type="checkbox"
        data-testid="input-changestream-relation-{relation.relationName}"
        class="bg-white-dark m-1 checkbox shrink-0"
        checked={relation.selected}
        disabled={isDisabled}
        onchange={(e) => {
          const follow = e.currentTarget.checked
          const relations = pipelinesRelations[tenantName][pipelineId]
          relations[relation.relationName].selected = follow
          if (follow) {
            // If stream is stopped - the action will silently fail
            startReadingStream(api, tenantName, pipelineId, relation.relationName)
          } else {
            relations[relation.relationName].cancelStream?.()
            relations[relation.relationName].cancelStream = undefined
            const isAnySelected = Object.values(relations).some(({ selected }) => selected)
            if (isAnySelected) {
              dropRelationRows(tenantName, pipelineId, relation.relationName)
            } else {
              changeStream[tenantName][pipelineId].rows = []
              changeStream[tenantName][pipelineId].headers = []
            }
            getChangeStream.current = changeStream
          }
        }}
        value={relation}
      />
      <span class="truncate">{relation.relationName}</span>
    </label>
    {#if isDisabled}
      <Tooltip class="" placement="right">
        Cannot follow more than {maxStreamsOnHttp} tables and views across all pipelines over HTTP. Consider
        using HTTPS (supported in Feldera Enterprise Edition).
      </Tooltip>
    {/if}
  {/snippet}
  {#if inputs.length}
    <div class="text-surface-600-400">Tables</div>
  {/if}
  {#each inputs as relation}
    {@render relationItem({ ...relation, pipelineName })}
  {/each}
  {#if outputs.length}
    <div class="pt-2 text-surface-600-400">Views</div>
  {/if}
  {#each outputs as relation}
    {@render relationItem({ ...relation, pipelineName })}
  {/each}
  {#if inputs.length + outputs.length === 0}
    <div class="text-surface-600-400">No relations</div>
  {/if}
{/snippet}

{#snippet dataView()}
  {#if getChangeStream.current[tenantName]?.[pipelineId]?.rows?.length}
    {#key `${tenantName}::${pipelineId}`}
      <ChangeStream
        changeStream={getChangeStream.current[tenantName][pipelineId]}
        onScrollPausedChange={(paused) => {
          if (paused) {
            pauseSelectedStreams(tenantName, pipelineId)
          } else {
            resumeSelectedStreams(api, tenantName, pipelineId)
          }
        }}
      ></ChangeStream>
    {/key}
  {:else if Object.values(pipelinesRelations[tenantName]?.[pipelineId] ?? {}).some((r) => r.selected)}
    <span class="p-2 text-surface-600-400">
      The selected tables and views have not emitted any new changes
    </span>
  {:else}
    <!-- The viewBox crops the drawing's empty margins, so it and the hint centre as one block. -->
    <div class="flex h-full flex-1 flex-col items-center justify-center gap-4 p-2 text-center">
      {#if darkMode.current === 'dark'}
        <ChangeStreamDark viewBox="30 40 216 104" class="h-[104px] w-[216px] max-w-full"
        ></ChangeStreamDark>
      {:else}
        <ChangeStreamLight viewBox="30 40 216 104" class="h-[104px] w-[216px] max-w-full"
        ></ChangeStreamLight>
      {/if}
      <span class="text-surface-600-400">
        Select tables and views to see the record updates as they are emitted
      </span>
    </div>
  {/if}
{/snippet}

{#if deleted}
  <div data-testid="box-changestream-banner">
    <WarningBanner variant="info">
      Change streaming is disabled. The pipeline has been deleted.
    </WarningBanner>
  </div>
{/if}
<!-- On wider screens the list of relations reaches the panel's left and bottom edges, past
     the 8px the tab content keeps from them, and a line divides it from the records. -->
<div class="flex flex-row {isMobile.current ? 'h-full' : '-mb-2 -ml-2 h-[calc(100%+0.5rem)]'}">
  {#if isMobile.current}
    <div
      class="bg-white-dark scrollbar flex flex-1 flex-col gap-1 overflow-y-auto rounded pt-2 pl-2 sm:gap-2 sm:p-2"
    >
      <SegmentedControl
        value={mobileDisplayMode}
        onValueChange={(v) => (mobileDisplayMode = v)}
        items={mobileDisplayItems}
      />
      {#if mobileDisplayMode === mobileDisplayModes[0]}
        {@render relationView()}
      {:else}
        <div class="scrollbar flex h-full overflow-y-auto">
          {@render dataView()}
        </div>
      {/if}
    </div>
  {:else}
    <PaneGroup direction={isMobile.current ? 'vertical' : 'horizontal'} onpaste={ingestPasted}>
      <Pane defaultSize={20} minSize={10} class="flex h-full">
        <div
          class="scrollbar flex w-full flex-col overflow-y-auto border-r border-line p-4 text-nowrap"
        >
          {@render relationView()}
        </div>
      </Pane>
      <PaneResizer
        class="my-2 sm:my-0 sm:mr-2 {isMobile.current
          ? 'pane-divider-horizontal'
          : 'pane-divider-vertical'}"
      ></PaneResizer>
      <Pane minSize={60} class="flex h-full">
        {@render dataView()}
      </Pane>
    </PaneGroup>
  {/if}
</div>
