<script lang="ts" context="module">
  let relationName = $state<Record<string, string>>({}) // Selected relation for each pipelineName
</script>

<script lang="ts">
  import { getCaseIndependentName } from '$lib/functions/felderaRelation'
  import type { ProgramSchema } from '$lib/services/pipelineManager'
  import { getExtendedPipeline } from '$lib/services/pipelineManager'
  import ChangeStream from './ChangeStream.svelte'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'

  let { pipelineName: _pipelineName }: { pipelineName: string } = $props()

  let pipelineName = $state(_pipelineName)
  $effect(() => {
    pipelineName = _pipelineName
  })
  let programSchema = $state<Record<string, ProgramSchema>>({})

  const reloadSchema = async () => {
    const schema = (await getExtendedPipeline(pipelineName)).program_info?.schema
    if (!schema) {
      return
    }
    programSchema[pipelineName] = schema
  }

  $effect(() => {
    let interval = setInterval(reloadSchema, 2000)
    reloadSchema()
    return () => {
      clearInterval(interval)
    }
  })

  let inputs = $derived(
    programSchema[pipelineName]?.inputs.map((i) => getCaseIndependentName(i)) ?? []
  )
  let outputs = $derived(
    programSchema[pipelineName]?.outputs.map((i) => getCaseIndependentName(i)) ?? []
  )
</script>

<div class="flex h-full flex-row">
  <PaneGroup direction="horizontal">
    <Pane defaultSize={20} minSize={5} class="flex h-full">
      <div class="flex w-full flex-col overflow-y-auto text-nowrap">
        {#snippet relationItem(relation: string)}
          <label class="flex-none overflow-hidden overflow-ellipsis">
            <input
              type="radio"
              class="  focus:ring-transparent"
              bind:group={relationName[pipelineName]}
              value={relation} />
            {relation}
          </label>
        {/snippet}
        {#if inputs.length}
          <div class="text-surface-500">Tables:</div>
        {/if}
        {#each inputs as relation}
          {@render relationItem(relation)}
        {/each}
        {#if outputs.length}
          <div class="text-surface-500">Views:</div>
        {/if}
        {#each outputs as relation}
          {@render relationItem(relation)}
        {/each}
        {#if inputs.length + outputs.length === 0}
          <div class="text-surface-500">No relations</div>
        {/if}
      </div>
    </Pane>
    <PaneResizer class="bg-surface-100-900 w-2"></PaneResizer>

    <Pane minSize={70} class="flex h-full">
      {#if relationName[pipelineName]}
        <ChangeStream {pipelineName} relationName={relationName[pipelineName]}></ChangeStream>
      {:else}
        <span class="text-surface-500 px-4">
          Select table or view to see the record updates in the data flow
        </span>
      {/if}
    </Pane>
  </PaneGroup>
</div>
