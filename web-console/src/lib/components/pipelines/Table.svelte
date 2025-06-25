<script lang="ts">
  import {
    Datatable,
    TableHandler,
    type TableHandlerInterface,
    type Field
  } from '@vincjo/datatables'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import {
    type PipelineStatus as PipelineStatusType,
    type PipelineThumb
  } from '$lib/services/pipelineManager'
  import { type Snippet } from 'svelte'
  import ThSort from '$lib/components/pipelines/table/ThSort.svelte'
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  let {
    pipelines,
    preHeaderEnd,
    selectedPipelines = $bindable()
  }: { pipelines: PipelineThumb[]; preHeaderEnd?: Snippet; selectedPipelines: string[] } = $props()

  const table = new TableHandler(pipelines, { rowsPerPage: undefined, selectBy: 'name' })
  $effect(() => {
    table.setRows(pipelines)
  })
  $effect(() => {
    selectedPipelines = table.selected as string[]
  })
  $effect(() => {
    table.selected = selectedPipelines
  })

  const statusFilter = table.createFilter('status')
  const filterStatuses: (PipelineStatusType | '')[] = ['', 'Running', 'Paused', 'Stopped']
</script>

<div
  class="relative mb-6 mt-5 flex h-10 flex-col items-center justify-end gap-4 sm:-mt-7 sm:flex-row md:mb-0"
>
  <select
    class="select ml-auto w-40 sm:ml-0"
    bind:value={statusFilter.value}
    onchange={() => statusFilter.set()}
  >
    {#each filterStatuses as status (status)}
      <option value={status}
        >{status === '' ? 'All pipelines' : getPipelineStatusLabel(status)}</option
      >
    {/each}
  </select>
  <div class="ml-auto flex gap-4 sm:ml-0">
    {@render preHeaderEnd?.()}
  </div>
</div>
<Datatable headless {table}>
  <table class="p-1">
    <thead>
      <tr>
        <th class="w-10 px-2 text-left"
          ><input
            class="checkbox"
            type="checkbox"
            checked={table.isAllSelected}
            onclick={() => table.selectAll()}
          /></th
        >
        <ThSort class="py-1" {table} field="name"
          ><span class="text-base font-normal text-surface-950-50">Pipeline name</span></ThSort
        >
        <ThSort {table} class="py-1" field="status"
          ><span class="text-base font-normal text-surface-950-50">Status</span></ThSort
        >
      </tr>
    </thead>
    <tbody>
      {#each table.rows as pipeline}
        <tr class="group"
          ><td class="px-2 border-surface-100-900 group-hover:bg-surface-50-950">
            <input
              class="checkbox"
              type="checkbox"
              checked={table.selected.includes(pipeline.name)}
              onclick={() => table.select(pipeline.name)}
            />
          </td>
          <td class="relative border-surface-100-900 group-hover:bg-surface-50-950"
            ><a
              class=" absolute top-2 w-full overflow-hidden overflow-ellipsis whitespace-nowrap"
              href="/pipelines/{pipeline.name}/">{pipeline.name}</a
            ></td
          >
          <td class="border-surface-100-900 group-hover:bg-surface-50-950"
            ><PipelineStatus status={pipeline.status}></PipelineStatus></td
          >
        </tr>
      {:else}
        <tr>
          <td></td>
          <td colspan={99} class="py-1">No pipelines with the specified status</td>
        </tr>
      {/each}
    </tbody>
  </table>
</Datatable>

<style lang="sass">

  td
    @apply py-1 text-base border-t-[0.5px]
</style>
