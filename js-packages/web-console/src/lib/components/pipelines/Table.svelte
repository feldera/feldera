<script lang="ts">
  import { Datatable, TableHandler } from '@vincjo/datatables'
  import type { Snippet } from 'svelte'
  import { match } from 'ts-pattern'
  import { pipe } from 'valibot'
  import { page } from '$app/state'
  import { Popover } from '$lib/components/common/Popover.svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import ThSort from '$lib/components/pipelines/table/ThSort.svelte'
  import { dateMax } from '$lib/functions/common/date'
  import { type NamesInUnion, unionName } from '$lib/functions/common/union'
  import { formatDateTime, useElapsedTime } from '$lib/functions/format'
  import type {
    PipelineStatus as PipelineStatusType,
    PipelineThumb
  } from '$lib/services/pipelineManager'
  import PipelineVersion from './table/PipelineVersion.svelte'

  let {
    pipelines,
    preHeaderEnd,
    selectedPipelines = $bindable()
  }: { pipelines: PipelineThumb[]; preHeaderEnd?: Snippet; selectedPipelines: string[] } = $props()
  const pipelinesWithLastChange = $derived(
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
  const td = 'py-1 text-base border-t-[0.5px]'
</script>

<div class="relative -mt-7 mb-6 flex flex-col items-center justify-end gap-4 md:mb-0 md:flex-row">
  <select
    class="h_-9 select ml-auto w-40 md:ml-0"
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
        <ThSort
          {table}
          class="w-20 py-1 pr-4 text-right xl:w-32"
          field={(p) => p.connectors?.numErrors}
        >
          <span class="text-base font-normal text-surface-950-50">
            <span class="inline xl:hidden">Errors</span>
            <span class="hidden xl:!inline">Runtime errors</span>
          </span>
        </ThSort>
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
          ><td class="{td} border-surface-100-900 px-2 group-hover:bg-surface-50-950">
            <input
              class="checkbox"
              type="checkbox"
              checked={table.selected.includes(pipeline.name)}
              onclick={() => table.select(pipeline.name)}
            />
          </td>
          <td class="{td} relative w-3/12 border-surface-100-900 group-hover:bg-surface-50-950"
            ><a
              class=" absolute top-2 w-full overflow-hidden overflow-ellipsis whitespace-nowrap"
              href="/pipelines/{pipeline.name}/">{pipeline.name}</a
            ></td
          >
          <td class="{td} relative w-12 border-surface-100-900 group-hover:bg-surface-50-950">
            <div
              class="fd {pipeline.storageStatus === 'Cleared'
                ? 'fd-database-off text-surface-500'
                : 'fd-database'} text-center text-[20px]"
            ></div>
            <Tooltip
              >{match(pipeline.storageStatus)
                .with('InUse', () => 'Storage in use')
                .with('Clearing', () => 'Clearing storage')
                .with('Cleared', () => 'Storage cleared')
                .exhaustive()}</Tooltip
            >
          </td>
          <td class="pr-2 {td} w-36 border-surface-100-900 group-hover:bg-surface-50-950"
            ><PipelineStatus status={pipeline.status}></PipelineStatus></td
          >
          <td
            class="{td} relative border-surface-100-900 whitespace-pre-wrap group-hover:bg-surface-50-950"
          >
            <span
              class="absolute top-1.5 w-full overflow-hidden align-middle overflow-ellipsis whitespace-nowrap"
            >
              {#if pipeline.deploymentError}
                {@const message = pipeline.deploymentError.message}
                <span class="fd fd-circle-alert pr-2 text-[20px] text-error-500"></span>
                <Popover class="z-10" strategy="fixed">
                  <div
                    class="scrollbar flex max-h-[50vh] max-w-[80vw] overflow-auto whitespace-pre-wrap"
                  >
                    {message}
                  </div>
                </Popover>
                {message.slice(0, ((idx) => (idx > 0 ? idx : undefined))(message.indexOf('\n')))}
              {/if}
            </span>
          </td>
          <td class="{td} border-surface-100-900 pr-4 group-hover:bg-surface-50-950">
            <div class="text-right text-nowrap">
              {pipeline.connectors?.numErrors ?? '-'}
            </div>
          </td>
          <td class="{td} relative border-surface-100-900 group-hover:bg-surface-50-950">
            <div class="flex w-full flex-nowrap items-center gap-2 text-nowrap">
              <PipelineVersion
                pipelineName={pipeline.name}
                runtimeVersion={pipeline.platformVersion}
                baseRuntimeVersion={page.data.feldera!.version}
                configuredRuntimeVersion={pipeline.programConfig.runtime_version}
              ></PipelineVersion>
            </div>
          </td>
          <td class="{td} relative w-28 border-surface-100-900 group-hover:bg-surface-50-950">
            <div class="w-32 text-right text-nowrap">
              {formatElapsedTime(pipeline.lastStatusSince, 'dhm')} ago
            </div>
          </td>
          <td class="{td} relative w-40 border-surface-100-900 group-hover:bg-surface-50-950">
            <div class="pr-1 text-right text-nowrap">
              {pipeline.deploymentResourcesStatus === 'Provisioned'
                ? formatDateTime(pipeline.deploymentResourcesStatusSince)
                : ''}
            </div>
          </td>
        </tr>
      {:else}
        <tr>
          <td class={td}></td>
          <td class={td} colspan={99}>No pipelines with the specified status</td>
        </tr>
      {/each}
    </tbody>
  </table>
</Datatable>
