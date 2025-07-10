<script lang="ts">
  import { Datatable, TableHandler } from '@vincjo/datatables'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import {
    type PipelineStatus as PipelineStatusType,
    type PipelineThumb
  } from '$lib/services/pipelineManager'
  import { type Snippet } from 'svelte'
  import ThSort from '$lib/components/pipelines/table/ThSort.svelte'
  import { useElapsedTime } from '$lib/functions/format'
  import { dateMax } from '$lib/functions/common/date'
  import { match } from 'ts-pattern'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { unionName, type NamesInUnion } from '$lib/functions/common/union'
  let {
    pipelines,
    preHeaderEnd,
    selectedPipelines = $bindable()
  }: { pipelines: PipelineThumb[]; preHeaderEnd?: Snippet; selectedPipelines: string[] } = $props()
  let pipelinesWithLastChange = $derived(
    pipelines.map((pipeline) => ({
      ...pipeline,
      lastStatusSince: dateMax(
        new Date(pipeline.deploymentStatusSince),
        new Date(pipeline.programStatusSince)
      )
    }))
  )

  // svelte-ignore state_referenced_locally
  const table = new TableHandler(pipelinesWithLastChange, {
    rowsPerPage: undefined,
    selectBy: 'name'
  })
  $effect(() => {
    table.setRows(pipelinesWithLastChange)
  })
  $effect(() => {
    selectedPipelines = table.selected as string[]
  })
  $effect(() => {
    table.selected = selectedPipelines
  })

  const statusMatchesFilter = (
    entry: NamesInUnion<PipelineStatusType>,
    _value: keyof typeof filterStatuses
  ) => {
    const value = filterStatuses.find((f) => f[0] === _value)![1]
    return !value.length || value.includes(entry)
  }
  const statusFilter = table.createFilter(
    (row) => unionName(row.status),
    statusMatchesFilter as any
  )

  const filterStatuses: [string, NamesInUnion<PipelineStatusType>[]][] = [
    ['All Pipelines', []],
    ['Running', ['Running']],
    ['Paused', ['Paused']],
    ['Ready To Start', ['Stopped']],
    ['Compiling', ['Queued', 'CompilingSql', 'SqlCompiled', 'CompilingRust']],
    ['Failed', ['SystemError', 'SqlError', 'RustError']]
  ]
  const { formatElapsedTime } = useElapsedTime()
</script>

<div
  class="relative mb-6 mt-5 flex h-10 flex-col items-center justify-end gap-4 sm:-mt-7 sm:flex-row md:mb-0"
>
  <select
    class="select ml-auto w-40 sm:ml-0"
    onchange={(e) => {
      statusFilter.value = filterStatuses.find((v) => e.currentTarget.value === v[0])![0]
      statusFilter.set()
    }}
  >
    {#each filterStatuses as filter (filter[0])}
      <option value={filter[0]}>{filter[0]}</option>
    {/each}
  </select>
  <div class="ml-auto flex gap-4 sm:ml-0">
    {@render preHeaderEnd?.()}
  </div>
</div>
<Datatable headless {table}>
  <table class="max-w-[1500px] p-1">
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
        <ThSort class="px-1 py-1" {table} field="name"
          ><span class="text-base font-normal text-surface-950-50">Pipeline name</span></ThSort
        >
        <th class="px-1 py-1 text-left"
          ><span class="text-base font-normal text-surface-950-50">Storage</span></th
        >
        <ThSort {table} class="px-1 py-1" field="status"
          ><span class="ml-8 text-base font-normal text-surface-950-50">Status</span></ThSort
        >
        <th class="px-1 py-1 text-left"
          ><span class="text-base font-normal text-surface-950-50">Message</span></th
        >
        <ThSort {table} class="px-1 py-1 " field="lastStatusSince"
          ><span class="text-base font-normal text-surface-950-50">Status changed</span></ThSort
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
          <td class="relative w-3/12 border-surface-100-900 group-hover:bg-surface-50-950"
            ><a
              class=" absolute top-2 w-full overflow-hidden overflow-ellipsis whitespace-nowrap"
              href="/pipelines/{pipeline.name}/">{pipeline.name}</a
            ></td
          >
          <td class="relative w-12 border-surface-100-900 group-hover:bg-surface-50-950">
            <div
              class="fd {pipeline.storageStatus === 'Cleared'
                ? 'fd-database-off text-surface-500'
                : 'fd-database'} text-center text-[20px]"
            ></div>
            <Tooltip class="bg-white-dark z-10 rounded text-surface-950-50"
              >{match(pipeline.storageStatus)
                .with('InUse', () => 'Storage in use')
                .with('Clearing', () => 'Clearing storage')
                .with('Cleared', () => 'Storage cleared')
                .exhaustive()}</Tooltip
            >
          </td>
          <td class="w-36 border-surface-100-900 group-hover:bg-surface-50-950"
            ><PipelineStatus status={pipeline.status}></PipelineStatus></td
          >
          <td
            class="relative whitespace-pre-wrap border-surface-100-900 group-hover:bg-surface-50-950"
          >
            <span
              class="absolute top-1.5 w-full overflow-hidden overflow-ellipsis whitespace-nowrap align-middle"
            >
              {#if pipeline.deploymentError}
                <span class="fd fd-circle-alert pr-2 text-[20px] text-error-500"></span>
                {pipeline.deploymentError?.message.slice(
                  0,
                  ((idx) => (idx > 0 ? idx : undefined))(
                    pipeline.deploymentError.message.indexOf('\n')
                  )
                )}
              {/if}
            </span>
          </td>
          <td class="relative w-28 border-surface-100-900 group-hover:bg-surface-50-950">
            <div class="w-32 text-nowrap text-right">
              {formatElapsedTime(pipeline.lastStatusSince, 'dhm')} ago
            </div>
          </td>
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
