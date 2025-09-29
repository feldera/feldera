<script lang="ts">
  import { Datatable, TableHandler } from '@vincjo/datatables'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import {
    type PipelineStatus as PipelineStatusType,
    type PipelineThumb
  } from '$lib/services/pipelineManager'
  import { type Snippet } from 'svelte'
  import ThSort from '$lib/components/pipelines/table/ThSort.svelte'
  import { formatDateTime, useElapsedTime } from '$lib/functions/format'
  import { dateMax } from '$lib/functions/common/date'
  import { match } from 'ts-pattern'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { unionName, type NamesInUnion } from '$lib/functions/common/union'
  import PipelineVersion from './table/PipelineVersion.svelte'
  import { page } from '$app/state'
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

<div class="relative -mt-7 mb-6 flex flex-col items-center justify-end gap-4 md:mb-0 md:flex-row">
  <select
    class="select ml-auto w-40 md:ml-0"
    onchange={(e) => {
      statusFilter.value = filterStatuses.find((v) => e.currentTarget.value === v[0])![0]
      statusFilter.set()
    }}
  >
    {#each filterStatuses as filter (filter[0])}
      <option value={filter[0]}>{filter[0]}</option>
    {/each}
  </select>
  <div class="ml-auto flex gap-4 md:ml-0">
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
        <ThSort {table} class="w-20 px-1 py-1 xl:w-32" field="platformVersion">
          <span class="text-base font-normal text-surface-950-50">
            Runtime <span class="hidden xl:!inline">version</span>
          </span>
        </ThSort>
        <ThSort {table} class="px-1 py-1" field="lastStatusSince"
          ><span class="text-base font-normal text-surface-950-50">Status changed</span></ThSort
        >
        <ThSort {table} class="px-1 py-1" field="deploymentResourcesStatusSince"
          ><span class="text-base font-normal text-surface-950-50">Deployed on</span></ThSort
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
                {@const message = pipeline.deploymentError.message}
                <span class="fd fd-circle-alert pr-2 text-[20px] text-error-500"></span>
                <Tooltip
                  class="bg-white-dark z-10 whitespace-pre-wrap rounded text-surface-950-50"
                  strategy="fixed"
                  activeContent
                >
                  <div class="flex max-h-[50vh] max-w-[80vw] overflow-auto whitespace-pre-wrap">
                    {message}
                  </div>
                </Tooltip>
                {message.slice(0, ((idx) => (idx > 0 ? idx : undefined))(message.indexOf('\n')))}
              {/if}
            </span>
          </td>
          <td class="relative border-surface-100-900 group-hover:bg-surface-50-950">
            <div class="flex flex-nowrap items-center w-full justify-end gap-2 text-nowrap text-right">
              <PipelineVersion
                runtimeVersion={pipeline.platformVersion}
                baseRuntimeVersion={page.data.feldera!.version}
              ></PipelineVersion>
            </div>
          </td>
          <td class="relative w-28 border-surface-100-900 group-hover:bg-surface-50-950">
            <div class="w-32 text-nowrap text-right">
              {formatElapsedTime(pipeline.lastStatusSince, 'dhm')} ago
            </div>
          </td>
          <td class="relative w-40 border-surface-100-900 group-hover:bg-surface-50-950">
            <div class="text-nowrap pr-1 text-right">
              {pipeline.deploymentResourcesStatus === 'Provisioned'
                ? formatDateTime(pipeline.deploymentResourcesStatusSince)
                : ''}
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
