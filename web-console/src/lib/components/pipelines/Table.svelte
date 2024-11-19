<script lang="ts">
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { Datatable, TableHandler, ThSort } from '@vincjo/datatables'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import { type PipelineStatus as PipelineStatusType } from '$lib/services/pipelineManager'
  import type { Snippet } from 'svelte'
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  let { preHeaderEnd }: { preHeaderEnd?: Snippet } = $props()
  const pipelines = usePipelineList()

  const table = new TableHandler(pipelines.pipelines, { rowsPerPage: 10, selectBy: 'name' })

  const statusFilter = table.createFilter('status')
  const filterStatuses: (PipelineStatusType | '')[] = ['', 'Running', 'Paused', 'Shutdown']
</script>

<div class="relative mb-6 mt-2 flex h-10 items-center justify-end gap-4 md:-mt-8 md:mb-0">
  <select class="select w-44" bind:value={statusFilter.value} onchange={() => statusFilter.set()}>
    {#each filterStatuses as status (status)}
      <option value={status}
        >{status === '' ? 'All pipelines' : getPipelineStatusLabel(status)}</option
      >
    {/each}
  </select>
  {@render preHeaderEnd?.()}
</div>
<Datatable {table}>
  <table>
    <thead>
      <tr>
        <th class="w-12 text-left"
          ><input
            class="checkbox"
            type="checkbox"
            checked={table.isAllSelected}
            onclick={() => table.selectAll()}
          /></th
        >

        <ThSort {table} field="name"
          ><span class="text-base font-normal">Pipeline name</span></ThSort
        >
        <ThSort {table} field="status"><span class="text-base font-normal">Status</span></ThSort>
      </tr>
    </thead>
    <tbody>
      {#each table.rows as pipeline}
        <tr>
          <td>
            <input
              class="checkbox"
              type="checkbox"
              checked={table.selected.includes(pipeline.name)}
              onclick={() => table.select(pipeline.name)}
            />
          </td>
          <td><a href="/pipelines/{pipeline.name}/">{pipeline.name}</a></td>
          <td><PipelineStatus status={pipeline.status}></PipelineStatus></td>
        </tr>
      {/each}
    </tbody>
  </table>
</Datatable>
