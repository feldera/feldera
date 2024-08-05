<script lang="ts" context="module">
  type RelationInfo = {
    pipelineName: string
    relationName: string
  }
  type ExtraType = {
    selected: boolean
    cancelStream?: () => void
  }
  let pipelinesRelations = $state<
    Record<
      string,
      Record<string, ExtraType & { type: 'tables' | 'views' }> // Record<'tables' | 'views', Record<string, ExtraType & { type: 'tables' | 'views' }>>
    >
  >({})
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  let rows = $state<
    // Record<string, { relationName: string; type: 'insert' | 'delete'; record: XgressRecord }[]>
    Record<
      string,
      ({ relationName: string } & ({ insert: XgressRecord } | { delete: XgressRecord }))[]
    >
  >({}) // Initialize row array

  const bufferSize = 1000
  const pushChanges =
    (pipelineName: string, relationName: string) =>
    (changes: Record<'insert' | 'delete', XgressRecord>[]) => {
      rows[pipelineName].splice(
        0,
        // Math.max(Math.min(
        rows[pipelineName].length + changes.length - bufferSize
        //   , bufferSize), 0)
      )
      rows[pipelineName].push(
        ...changes.slice(-bufferSize).map((change) => ({
          // type: 'insert' in change ? ('insert' as const) : ('delete' as const),
          ...change,
          relationName
          // record: change.insert ?? change.delete
        }))
      )
    }
  const startReadingStream = (pipelineName: string, relationName: string) => {
    const handle = relationEggressStream(pipelineName, relationName).then((stream) => {
      if (!stream) {
        return undefined
      }
      const reader = stream.getReader()
      accumulateChanges(reader, pushChanges(pipelineName, relationName))
      return () => reader.cancel('not_needed')
    })
    return () => {
      handle.then((cancel) => cancel?.())
      rows[pipelineName] = rows[pipelineName].filter((row) => row.relationName !== relationName)
    }
  }
  const registerPipelineName = (pipelineName: string) => {
    if (pipelinesRelations[pipelineName]) {
      return
    }
    pipelinesRelations[pipelineName] = {}
    rows[pipelineName] = []
    pipelineActionCallbacks.add(pipelineName, 'start_paused', async () => {
      const relations = Object.entries(pipelinesRelations[pipelineName])
        .filter((relation) => relation[1].selected)
        .map((relation) => relation[0])
      for (const relationName of relations) {
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
  import { getExtendedPipeline, relationEggressStream } from '$lib/services/pipelineManager'
  import type { XgressRecord } from '$lib/types/pipelineManager'
  import ChangeStream from './ChangeStream.svelte'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import type { Relation } from '$lib/services/manager'
  import { accumulateChanges } from '$lib/functions/pipelines/changeStream'

  let { pipelineName: _pipelineName }: { pipelineName: string } = $props()

  let pipelineName = $state(_pipelineName)
  $effect(() => {
    pipelineName = _pipelineName
  })

  const reloadSchema = async () => {
    registerPipelineName(pipelineName)
    const schema = (await getExtendedPipeline(pipelineName)).program_info?.schema
    if (!schema) {
      return
    }
    const process = (type: 'tables' | 'views', newRelations: Relation[]) => {
      for (const newRelation of newRelations) {
        const newRelationName = getCaseIndependentName(newRelation)
        const oldRelation = pipelinesRelations[pipelineName][newRelationName]
        if (!oldRelation) {
          pipelinesRelations[pipelineName][newRelationName] = {
            selected: false,
            type
          }
        }
      }
    }
    process('tables', schema.inputs)
    process('views', schema.outputs)
  }

  $effect(() => {
    let interval = setInterval(reloadSchema, 2000)
    reloadSchema()
    return () => {
      clearInterval(interval)
    }
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
</script>

<div class="flex h-full flex-row">
  <PaneGroup direction="horizontal">
    <Pane defaultSize={20} minSize={5} class="flex h-full">
      <div class="flex w-full flex-col overflow-y-auto text-nowrap">
        {#snippet relationItem(relation: RelationInfo & ExtraType)}
          <label class="flex-none overflow-hidden overflow-ellipsis">
            <input
              type="checkbox"
              class="focus:ring-transparent"
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
          <div class="text-surface-500">Tables:</div>
        {/if}
        {#each inputs as relation}
          {@render relationItem({ ...relation, pipelineName })}
        {/each}
        {#if outputs.length}
          <div class="text-surface-500">Views:</div>
        {/if}
        {#each outputs as relation}
          {@render relationItem({ ...relation, pipelineName })}
        {/each}
        {#if inputs.length + outputs.length === 0}
          <div class="text-surface-500">No relations</div>
        {/if}
      </div>
    </Pane>
    <PaneResizer class="w-2 bg-surface-100-900"></PaneResizer>

    <Pane minSize={70} class="flex h-full">
      {#if rows[pipelineName]}
        <ChangeStream changes={rows[pipelineName]}></ChangeStream>
      {:else}
        <span class="px-4 text-surface-500">
          Select table or view to see the record updates in the data flow
        </span>
      {/if}
    </Pane>
  </PaneGroup>
</div>
