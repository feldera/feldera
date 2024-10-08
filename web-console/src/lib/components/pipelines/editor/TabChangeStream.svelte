<script lang="ts" module>
  type RelationInfo = {
    pipelineName: string
    relationName: string
  }
  type ExtraType = {
    selected: boolean
    cancelStream?: () => void
  }
  let pipelinesRelations = $state<
    Record<string, Record<string, ExtraType & { type: 'tables' | 'views' }>>
  >({})
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  type Payload = { insert: XgressRecord } | { delete: XgressRecord } | { skippedBytes: number }
  type Row = { relationName: string } & Payload
  let changeStream: Record<string, { rows: Row[]; totalSkippedBytes: number }> = {} // Initialize row array
  // Separate getRows as a $state avoids burdening rows array itself with reactivity overhead
  let getChangeStream = $state(() => changeStream)

  const bufferSize = 10000
  const pushChanges = (pipelineName: string, relationName: string) => (changes: Payload[]) => {
    changeStream[pipelineName].rows.splice(
      0,
      changeStream[pipelineName].rows.length + changes.length - bufferSize
    )
    changeStream[pipelineName].rows.push(
      ...changes.slice(-bufferSize).map((change) => ({
        ...change,
        relationName
      }))
    )
  }
  const startReadingStream = (pipelineName: string, relationName: string) => {
    const handle = relationEggressStream(pipelineName, relationName).then((stream) => {
      if ('message' in stream) {
        return undefined
      }
      const { cancel } = parseUTF8JSON(
        stream,
        pushChanges(pipelineName, relationName),
        (skippedBytes) => {
          pushChanges(pipelineName, relationName)([{ skippedBytes }])
          changeStream[pipelineName].totalSkippedBytes += skippedBytes
        },
        {
          paths: ['$.json_data.*'],
          bufferSize: 10 * 1024 * 1024
        }
      )
      return () => {
        cancel()
      }
    })
    return () => {
      handle.then((cancel) => cancel?.())
      changeStream[pipelineName].rows = changeStream[pipelineName].rows.filter(
        (row) => row.relationName !== relationName
      )
      getChangeStream = () => changeStream
    }
  }
  const registerPipelineName = (pipelineName: string) => {
    if (pipelinesRelations[pipelineName]) {
      return
    }
    pipelinesRelations[pipelineName] = {}
    changeStream[pipelineName] = { rows: [], totalSkippedBytes: 0 }
    pipelineActionCallbacks.add(pipelineName, 'start_paused', async () => {
      const relations = Object.entries(pipelinesRelations[pipelineName])
        .filter((relation) => relation[1].selected)
        .map((relation) => relation[0])
      for (const relationName of relations) {
        changeStream[pipelineName] = { rows: [], totalSkippedBytes: 0 } // Clear row buffer when starting pipeline again
        getChangeStream = () => changeStream
        pipelinesRelations[pipelineName][relationName].cancelStream = startReadingStream(
          pipelineName,
          relationName
        )
      }
    })
  }
</script>

<script lang="ts">
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'

  import { getCaseIndependentName } from '$lib/functions/felderaRelation'
  import {
    relationEggressStream,
    relationIngress,
    type ExtendedPipeline,
    type XgressEntry
  } from '$lib/services/pipelineManager'
  import type { XgressRecord } from '$lib/types/pipelineManager'
  import ChangeStream from './ChangeStream.svelte'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import type { Relation } from '$lib/services/manager'
  import { parseUTF8JSON } from '$lib/functions/pipelines/changeStream'
  import JSONbig from 'true-json-bigint'
  import { groupBy } from '$lib/functions/common/array'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  let pipelineName = $derived(pipeline.current.name)

  const reloadSchema = async (pipelineName: string, pipeline: ExtendedPipeline) => {
    const schema = pipeline.programInfo?.schema
    if (!schema) {
      return
    }
    registerPipelineName(pipelineName)
    const oldSchema = pipelinesRelations[pipelineName]
    pipelinesRelations[pipelineName] = {}
    const process = (type: 'tables' | 'views', newRelations: Relation[]) => {
      for (const newRelation of newRelations) {
        const newRelationName = getCaseIndependentName(newRelation)
        const oldRelation = oldSchema[newRelationName]?.type === type && oldSchema[newRelationName]
        pipelinesRelations[pipelineName][newRelationName] = oldRelation || {
          type,
          selected: false
        }
      }
    }
    process('tables', schema.inputs)
    process('views', schema.outputs)
  }

  $effect(() => {
    void pipeline.current
    setTimeout(() => reloadSchema(pipelineName, pipeline.current))
  })

  let inputs = $derived(
    Object.entries(pipelinesRelations[pipelineName] ?? {})
      .filter((e) => e[1].type === 'tables')
      .map(([relationName, value]) => ({
        relationName,
        ...value
      }))
  )
  let outputs = $derived(
    Object.entries(pipelinesRelations[pipelineName] ?? {})
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

  const pasteChanges = (changes: { relationName: string; values: XgressEntry[] }[]) => {
    for (const batch of changes) {
      relationIngress(pipelineName, batch.relationName, batch.values, 'force')
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
</script>

<div class="flex h-full flex-row">
  <PaneGroup direction="horizontal" onpaste={ingestPasted}>
    <Pane defaultSize={20} minSize={5} class="flex h-full p-2 pr-0">
      <div class="flex w-full flex-col overflow-y-auto text-nowrap">
        {#snippet relationItem(relation: RelationInfo & ExtraType)}
          <label class="flex-none overflow-hidden overflow-ellipsis">
            <input
              type="checkbox"
              class="bg-white-black focus:ring-transparent"
              checked={relation.selected}
              onchange={(e) => {
                const follow = e.currentTarget.checked
                pipelinesRelations[pipelineName] /*[relation.type]*/[
                  relation.relationName
                ].selected = follow
                if (!follow) {
                  pipelinesRelations[pipelineName][relation.relationName].cancelStream?.()
                }
                if (follow) {
                  // If stream is stopped - the action will silently fail
                  pipelinesRelations[pipelineName][relation.relationName].cancelStream =
                    startReadingStream(pipelineName, relation.relationName)
                }
              }}
              value={relation}
            />
            {relation.relationName}
          </label>
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
      </div>
    </Pane>
    <PaneResizer class="pane-divider-vertical bg-surface-100-900 "></PaneResizer>

    <Pane minSize={70} class="flex h-full">
      {#if getChangeStream()[pipelineName]?.rows?.length}
        <ChangeStream changeStream={getChangeStream()[pipelineName]}></ChangeStream>
      {:else}
        <span class="p-2 text-surface-600-400">
          {#if Object.values(pipelinesRelations[pipelineName] ?? {}).some((r) => r.selected)}
            The selected tables and views have not emitted any new changes
          {:else}
            Select tables and views to see the record updates as they are emitted
          {/if}
        </span>
      {/if}
    </Pane>
  </PaneGroup>
</div>
