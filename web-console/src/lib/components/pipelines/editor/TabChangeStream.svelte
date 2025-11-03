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
   * 3. When pipelineName/tenantName changes AND the pipeline is interactive (Running/Paused)
   *
   * Stream Stop/Cleanup Conditions:
   * 1. When a relation checkbox is unchecked by the user
   * 2. When pipelineName or tenantName changes (cleanup callback cancels old pipeline streams)
   * 3. When pipeline is deleted (via 'delete' action callback)
   *
   * Pipeline Interactive States (can stream data):
   * - Running, Paused, Pausing, Resuming
   *
   * Key Implementation Details:
   * - startSelectedStreams(): Starts streams for all selected relations, checks isPipelineInteractive
   * - Effect on pipelineName/tenantName: Non-reactive to isInteractive, only starts if interactive
   * - 'start_paused' callback: Always attempts to start streams (pipeline state guarantees validity)
   * - Each stream has a cancelStream() function stored in pipelinesRelations for cleanup
   * - Buffer clearing happens in startSelectedStreams to reset data when pipeline restarts
   */

  import type { ChangeStreamData, Row } from '$lib/components/pipelines/editor/ChangeStream.svelte'
  type RelationInfo = {
    pipelineName: string
    relationName: string
  }
  type ExtraType = {
    fields: Record<string, Field>
    selected: boolean
    cancelStream?: () => void
  }

  let pipelinesRelations = $state<
    Record<string, Record<string, Record<string, ExtraType & { type: 'tables' | 'views' }>>>
  >({})
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  let changeStream: Record<string, Record<string, ChangeStreamData>> = {} // Initialize row array nested by tenant and pipeline
  // Separate getRows as a $state avoids burdening rows array itself with reactivity overhead
  let getChangeStream = $state(() => changeStream)

  const bufferSize = 10000
  const filterOutRows = (rows: Row[], headers: number[], relationName: string) => {
    let batchRelationName: string | undefined = undefined
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
    pipelineName: string,
    relationName: string
  ) => {
    const request = api.relationEgressStream(pipelineName, relationName).then((result) => {
      if (result instanceof Error) {
        pipelinesRelations[tenantName][pipelineName][relationName].cancelStream = undefined
        return undefined
      }
      const { cancel } = parseCancellable(
        result,
        {
          pushChanges: (rows: XgressEntry[]) => {
            const initialLen = changeStream[tenantName][pipelineName].rows.length
            const lastRelationName = ((headerIdx) =>
              headerIdx !== undefined
                ? ((header) => (header && 'relationName' in header ? header.relationName : null))(
                    changeStream[tenantName][pipelineName].rows[headerIdx]
                  )
                : null)(changeStream[tenantName][pipelineName].headers.at(-1))
            const offset = pushAsCircularBuffer(
              () => changeStream[tenantName][pipelineName].rows,
              bufferSize,
              (v: Row) => v
            )(
              [
                ...(relationName !== lastRelationName
                  ? ([
                      {
                        relationName,
                        columns: Object.keys(
                          ((row) => ('insert' in row ? row.insert : row.delete))(rows[0])
                        ).map((name) => {
                          return pipelinesRelations[tenantName][pipelineName][relationName].fields[
                            normalizeCaseIndependentName({ name })
                          ]
                        })
                      }
                    ] as Row[])
                  : [])
              ].concat(rows)
            )
            if (relationName !== lastRelationName) {
              changeStream[tenantName][pipelineName].headers.push(initialLen)
            }
            changeStream[tenantName][pipelineName].headers = changeStream[tenantName][pipelineName].headers
              .map((i) => i - offset)
              .filter((i) => i >= 0)
          },
          onBytesSkipped: (skippedBytes) => {
            pushAsCircularBuffer(
              () => changeStream[tenantName][pipelineName].rows,
              bufferSize,
              (v) => v
            )([{ relationName, skippedBytes }])
            changeStream[tenantName][pipelineName].totalSkippedBytes += skippedBytes
          },
          onParseEnded: () =>
            (pipelinesRelations[tenantName][pipelineName][relationName].cancelStream = undefined)
        },
        new CustomJSONParserTransformStream<XgressEntry>({
          paths: ['$.json_data.*'],
          separator: ''
        }),
        {
          bufferSize: 8 * 1024 * 1024
        }
      )
      return () => {
        cancel()
      }
    })
    return () => {
      request.then((cancel) => {
        cancel?.()
        pipelinesRelations[tenantName][pipelineName][relationName].cancelStream = undefined
        ;({ rows: changeStream[tenantName][pipelineName].rows, headers: changeStream[tenantName][pipelineName].headers } =
          filterOutRows(
            changeStream[tenantName][pipelineName].rows,
            changeStream[tenantName][pipelineName].headers,
            relationName
          ))
        getChangeStream = () => changeStream
      })
    }
  }
  const startSelectedStreams = (api: PipelineManagerApi, tenantName: string, pipelineName: string) => {
    changeStream[tenantName][pipelineName] = { rows: [], headers: [], totalSkippedBytes: 0 } // Clear row buffer when starting pipeline again
    const relations = Object.entries(pipelinesRelations[tenantName]?.[pipelineName] ?? {})
      .filter((relation) => relation[1].selected)
      .map((relation) => relation[0])
    for (const relationName of relations) {
      if (pipelinesRelations[tenantName][pipelineName][relationName].cancelStream) {
        continue
      }
      pipelinesRelations[tenantName][pipelineName][relationName].cancelStream = startReadingStream(
        api,
        tenantName,
        pipelineName,
        relationName
      )
    }
    getChangeStream = () => changeStream
  }
  const registerPipelineName = (api: PipelineManagerApi, tenantName: string, pipelineName: string) => {
    if (!pipelinesRelations[tenantName]) {
      pipelinesRelations[tenantName] = {}
    }
    if (pipelinesRelations[tenantName][pipelineName]) {
      return
    }
    pipelinesRelations[tenantName][pipelineName] = {}
    if (!changeStream[tenantName]) {
      changeStream[tenantName] = {}
    }
    changeStream[tenantName][pipelineName] = { rows: [], headers: [], totalSkippedBytes: 0 }
    pipelineActionCallbacks.add(pipelineName, 'start_paused', async () => {
      startSelectedStreams(api, tenantName, pipelineName)
    })
  }
  const dropChangeStreamHistory = async (tenantName: string, pipelineName: string) => {
    if (pipelinesRelations[tenantName]) {
      delete pipelinesRelations[tenantName][pipelineName]
    }
    if (changeStream[tenantName]) {
      delete changeStream[tenantName][pipelineName]
    }
  }
</script>

<script lang="ts">
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'

  import {
    getCaseIndependentName,
    normalizeCaseIndependentName
  } from '$lib/functions/felderaRelation'
  import { type ExtendedPipeline, type XgressEntry } from '$lib/services/pipelineManager'
  import ChangeStream from './ChangeStream.svelte'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import type { Field, Relation } from '$lib/services/manager'
  import {
    CustomJSONParserTransformStream,
    parseCancellable,
    pushAsCircularBuffer
  } from '$lib/functions/pipelines/changeStream'
  import JSONbig from 'true-json-bigint'
  import { count, groupBy } from '$lib/functions/common/array'
  import { untrack } from 'svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import { useIsMobile } from '$lib/compositions/layout/useIsMobile.svelte'
  import { Segment } from '@skeletonlabs/skeleton-svelte'
  import {
    usePipelineManager,
    type PipelineManagerApi
  } from '$lib/compositions/usePipelineManager.svelte'
  import { useProtocol } from '$lib/compositions/useProtocol'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { getSelectedTenant } from '$lib/services/auth'
  import { isPipelineInteractive } from '$lib/functions/pipelines/status'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  let pipelineName = $derived(pipeline.current.name)
  let tenantName = $derived(getSelectedTenant() || '')
  let isInteractive = $derived(isPipelineInteractive(pipeline.current.status))

  const protocol = useProtocol()
  const maxStreamsOnHttp = 4

  let selectedRelationsCount = $derived(
    count(Object.values(pipelinesRelations), (tenantRelations) =>
      count(Object.values(tenantRelations), (relations) =>
        count(Object.values(relations), (r) => r.selected)
      )
    )
  )

  const reloadSchema = async (tenantName: string, pipelineName: string, pipeline: ExtendedPipeline) => {
    const schema = pipeline.programInfo?.schema
    if (!schema) {
      return
    }
    registerPipelineName(api, tenantName, pipelineName)
    const oldSchema = pipelinesRelations[tenantName]?.[pipelineName]
    if (!pipelinesRelations[tenantName]) {
      pipelinesRelations[tenantName] = {}
    }
    pipelinesRelations[tenantName][pipelineName] = {}
    const process = (type: 'tables' | 'views', newRelations: Relation[]) => {
      for (const newRelation of newRelations) {
        const newRelationName = getCaseIndependentName(newRelation)
        const oldRelation = oldSchema?.[newRelationName]?.type === type && oldSchema[newRelationName]
        pipelinesRelations[tenantName][pipelineName][newRelationName] = oldRelation || {
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

  $effect(() => {
    void pipeline.current
    untrack(() => reloadSchema(tenantName, pipelineName, pipeline.current))
  })

  $effect(() => {
    pipelineName
    tenantName

    const oldPipelineName = pipelineName

    // Start streams for any selected relations when pipeline is interactive
    untrack(() => {
      if (isInteractive) {
        startSelectedStreams(api, tenantName, pipelineName)
      }
    })

    return () => {
      // Clean up active streams when pipelineName changes
      const relations = pipelinesRelations[tenantName]?.[oldPipelineName]
      if (relations) {
        for (const relation of Object.values(relations)) {
          relation.cancelStream?.()
        }
      }
    }
  })

  let inputs = $derived(
    Object.entries(pipelinesRelations[tenantName]?.[pipelineName] ?? {})
      .filter((e) => e[1].type === 'tables')
      .map(([relationName, value]) => ({
        relationName,
        ...value
      }))
  )
  let outputs = $derived(
    Object.entries(pipelinesRelations[tenantName]?.[pipelineName] ?? {})
      .filter((e) => e[1].type === 'views')
      .map(([relationName, value]) => ({
        relationName,
        ...value
      }))
  )

  const visualUpdateMs = 100
  // Update visible list of changes at a constant time period
  $effect(() => {
    const update = () => (getChangeStream = () => changeStream)
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

  const api = usePipelineManager()
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
  const mobileDisplayModes = ['Tables and Views', 'Data stream'] as const
  let mobileDisplayMode = $state<(typeof mobileDisplayModes)[number]>('Tables and Views')
</script>

{#snippet relationView()}
  {#snippet relationItem(relation: RelationInfo & ExtraType)}
    {@const isDisabled =
      protocol === 'http' && !relation.selected && selectedRelationsCount >= maxStreamsOnHttp}
    <label
      class="flex-none overflow-hidden overflow-ellipsis {isDisabled
        ? 'cursor-not-allowed opacity-50'
        : 'cursor-pointer'}"
    >
      <input
        type="checkbox"
        class="bg-white-dark checkbox m-1"
        checked={relation.selected}
        disabled={isDisabled}
        onchange={(e) => {
          const follow = e.currentTarget.checked
          pipelinesRelations[tenantName][pipelineName][relation.relationName].selected = follow
          if (follow) {
            // If stream is stopped - the action will silently fail
            pipelinesRelations[tenantName][pipelineName][relation.relationName].cancelStream =
              startReadingStream(api, tenantName, pipelineName, relation.relationName)
          } else {
            pipelinesRelations[tenantName][pipelineName][relation.relationName].cancelStream?.()
            pipelinesRelations[tenantName][pipelineName][relation.relationName].cancelStream = undefined
            if (!Object.values(pipelinesRelations[tenantName][pipelineName]).some(({ selected }) => selected)) {
              changeStream[tenantName][pipelineName].rows = []
              changeStream[tenantName][pipelineName].headers = []
              getChangeStream = () => changeStream
              return
            }
            ;({
              rows: changeStream[tenantName][pipelineName].rows,
              headers: changeStream[tenantName][pipelineName].headers
            } = filterOutRows(
              changeStream[tenantName][pipelineName].rows,
              changeStream[tenantName][pipelineName].headers,
              relation.relationName
            ))
            getChangeStream = () => changeStream
          }
        }}
        value={relation}
      />
      {relation.relationName}
    </label>
    {#if isDisabled}
      <Tooltip class="z-10 bg-white text-surface-950-50 dark:bg-black" placement="right">
        Cannot follow more than {maxStreamsOnHttp} tables and views across all pipelines over HTTP. Consider
        using HTTPS (supported in Feldera Enterprise Edition).
      </Tooltip>
    {/if}
  {/snippet}
  {#if inputs.length}
    <div class="text-surface-600-400">Tables:</div>
  {/if}
  {#each inputs as relation}
    {@render relationItem({ ...relation, pipelineName })}
  {/each}
  {#if outputs.length}
    <div class="text-surface-600-400">Views:</div>
  {/if}
  {#each outputs as relation}
    {@render relationItem({ ...relation, pipelineName })}
  {/each}
  {#if inputs.length + outputs.length === 0}
    <div class="text-surface-600-400">No relations</div>
  {/if}
{/snippet}

{#snippet dataView()}
  {#if getChangeStream()[tenantName]?.[pipelineName]?.rows?.length}
    {#key `${tenantName}::${pipelineName}`}
      <ChangeStream changeStream={getChangeStream()[tenantName][pipelineName]}></ChangeStream>
    {/key}
  {:else}
    <span class="p-2 text-surface-600-400">
      {#if Object.values(pipelinesRelations[tenantName]?.[pipelineName] ?? {}).some((r) => r.selected)}
        The selected tables and views have not emitted any new changes
      {:else}
        Select tables and views to see the record updates as they are emitted
      {/if}
    </span>
  {/if}
{/snippet}

<div class="flex h-full flex-row">
  {#if isMobile.current}
    <div
      class="bg-white-dark flex flex-1 flex-col gap-1 overflow-y-auto rounded pl-2 pt-2 scrollbar sm:gap-2 sm:p-2"
    >
      <Segment
        bind:value={mobileDisplayMode}
        background="preset-filled-surface-50-950 w-fit flex-none"
        indicatorBg="bg-white-dark shadow"
        indicatorText=""
        border="p-1"
        rounded="rounded"
      >
        {#each mobileDisplayModes as mode}
          <Segment.Item value={mode} base="btn cursor-pointer z-[1] px-5 h-6 text-sm">
            {mode}
          </Segment.Item>
        {/each}
      </Segment>
      {#if mobileDisplayMode === mobileDisplayModes[0]}
        {@render relationView()}
      {:else}
        <div class="flex h-full overflow-y-auto scrollbar">
          {@render dataView()}
        </div>
      {/if}
    </div>
  {:else}
    <PaneGroup direction={isMobile.current ? 'vertical' : 'horizontal'} onpaste={ingestPasted}>
      <Pane defaultSize={20} minSize={10} class="flex h-full">
        <div
          class="bg-white-dark flex w-full flex-col gap-1 overflow-y-auto text-nowrap rounded p-4 scrollbar"
        >
          {@render relationView()}
        </div>
      </Pane>
      <PaneResizer
        class="my-2 sm:mx-2 sm:my-0 {isMobile.current
          ? 'pane-divider-horizontal'
          : 'pane-divider-vertical'}"
      ></PaneResizer>
      <Pane minSize={60} class="flex h-full">
        {@render dataView()}
      </Pane>
    </PaneGroup>
  {/if}
</div>
