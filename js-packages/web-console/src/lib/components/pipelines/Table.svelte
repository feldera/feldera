<script lang="ts">
  import { TableHandler } from '@vincjo/datatables'
  import { Popover, Select, Tooltip } from 'common-ui'
  import { match } from 'ts-pattern'
  import { page } from '$app/state'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import ThSort from '$lib/components/pipelines/table/ThSort.svelte'
  import { useElapsedTime } from '$lib/compositions/common/useElapsedTime'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { dateMax } from '$lib/functions/common/date'
  import { matchesSubstring } from '$lib/functions/common/string'
  import { type NamesInUnion, unionName } from '$lib/functions/common/union'
  import { formatDateTime } from '$lib/functions/format'
  import { resolve } from '$lib/functions/svelte'
  import type {
    PipelineStatus as PipelineStatusType,
    PipelineThumb
  } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'
  import PipelineVersion from './table/PipelineVersion.svelte'
  import Tags from './table/Tags.svelte'

  let {
    pipelines,
    header,
    preHeaderEnd,
    selectedPipelines = $bindable()
  }: {
    pipelines: PipelineThumb[]
    header?: Snippet
    preHeaderEnd?: Snippet
    selectedPipelines: string[]
  } = $props()

  let controlsHeight = $state(0)

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
    table.setRows(pipelinesFiltered)
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

  let nameSearch = $state('')
  const pipelinesFiltered = $derived(
    pipelinesWithLastChange.filter((p) => matchesSubstring(p.name, nameSearch))
  )

  // `knownTags` is the union of tags over every pipeline — the pool the per-row
  // tag picker chooses from.
  const knownTags = $derived.by(() => {
    const tags = new Set<string>()
    for (const pipeline of pipelines) {
      for (const tag of pipeline.tags) {
        tags.add(tag)
      }
    }
    return tags
  })

  const api = usePipelineManager()

  const { formatElapsedTime } = useElapsedTime()
  const td = 'h-10 text-base border-t-[0.5px]'
  // The last row also gets a bottom border, closing off the table instead of
  // leaving its final row's underside open.
  const rowTd = `${td} group-last:border-b-[0.5px]`

  // Persist the active sort and restore it on the matching column. Each ThSort
  // owns the upstream `direction`/`onSort` API; the column identity lives here.
  const { pipelinesTableSort } = useLayoutSettings()
  const sortColumn = (column: string) => ({
    direction:
      pipelinesTableSort.value.column === column ? pipelinesTableSort.value.direction : undefined,
    onSort: (direction: 'asc' | 'desc') => (pipelinesTableSort.value = { column, direction })
  })
</script>

<div class="pipeline-table-wrapper bg-white-dark w-fit min-w-full">
  <div class="bg-white-dark sticky top-0 z-10 pb-2" bind:clientHeight={controlsHeight}>
    <div class="sticky left-0 max-w-[100cqi] px-2 md:px-8">
      {#if header}
        {@render header()}
      {/if}
      <div
        class="relative mt-2 flex flex-row items-center gap-2 sm:justify-end sm:gap-4"
        class:lg:-mt-7={!!header}
        class:lg:mb-0={!!header}
      >
        <input
          data-testid="input-pipeline-search"
          class="input h-9 sm:w-60"
          type="search"
          placeholder="Search pipelines..."
          oninput={(e) => {
            nameSearch = e.currentTarget.value
          }}
        />
        <Select
          data-testid="select-pipeline-status"
          class="h-9 text-base! sm:w-40"
          onchange={(e) => {
            statusFilter.value = filterStatuses.find((v) => e.currentTarget.value === v[0])![0]
            statusFilter.set()
          }}
        >
          {#each filterStatuses as filter (filter[0])}
            <option value={filter[0]}>{filter[0]}</option>
          {/each}
        </Select>
        {@render preHeaderEnd?.()}
      </div>
    </div>
  </div>
  <div class="flex md:px-6">
    <table class="w-full border-separate border-spacing-0">
      <thead class="bg-white-dark sticky" style="top: {controlsHeight}px; z-index: 1;">
        <tr>
          <th class="w-10 px-3 text-left"
            ><div class="flex h-full items-center">
              <input
                class="checkbox"
                type="checkbox"
                checked={table.isAllSelected}
                onclick={() => table.selectAll()}
              />
            </div></th
          >
          <ThSort class="h-10 px-3" {table} field="name" {...sortColumn('name')}
            ><span class="text-base font-normal text-surface-950-50">Pipeline name</span></ThSort
          >
          <th class="h-10 px-3 text-left"
            ><span class="text-base font-normal text-surface-950-50">Storage</span></th
          >
          <ThSort {table} class="h-10 justify-center px-3" field="status" {...sortColumn('status')}
            ><span class="text-base font-normal text-surface-950-50">Status</span></ThSort
          >
          <th class="h-10 px-3 text-left"
            ><span class="text-base font-normal text-surface-950-50">Message</span></th
          >
          <th class="h-10 px-3 text-left"
            ><span class="text-base font-normal text-surface-950-50">Tags</span></th
          >
          <ThSort
            {table}
            class="h-10 w-20 px-3 xl:w-32"
            field="platformVersion"
            {...sortColumn('platformVersion')}
          >
            <span class="text-base font-normal text-surface-950-50">
              Runtime <span class="hidden xl:!inline">version</span>
            </span>
          </ThSort>
          <ThSort
            {table}
            class="h-10 w-20 justify-end px-3 xl:w-32"
            field={(p) => p.connectors?.numErrors}
            {...sortColumn('numErrors')}
          >
            <span class="text-base font-normal text-surface-950-50">
              <span class="inline xl:hidden">Errors</span>
              <span class="hidden xl:!inline">Runtime errors</span>
            </span>
          </ThSort>
          <ThSort
            {table}
            class="h-10 px-3"
            field="lastStatusSince"
            {...sortColumn('lastStatusSince')}
            ><span class="text-base font-normal text-surface-950-50">Status changed</span></ThSort
          >
          <ThSort
            {table}
            class="h-10 px-3"
            field="deploymentResourcesStatusSince"
            {...sortColumn('deploymentResourcesStatusSince')}
            ><span class="text-base font-normal text-surface-950-50">Deployed on</span></ThSort
          >
        </tr>
      </thead>
      <tbody>
        {#each table.rows as pipeline}
          <tr class="group" data-testid="box-row-{pipeline.name}"
            ><td class="{rowTd} border-surface-100-900 px-3 group-hover:bg-surface-50-950">
              <div class="flex h-full items-center">
                <input
                  class="checkbox"
                  type="checkbox"
                  checked={table.selected.includes(pipeline.name)}
                  onclick={() => table.select(pipeline.name)}
                />
              </div>
            </td>
            <td class="{rowTd} relative w-3/12 border-surface-100-900 group-hover:bg-surface-50-950"
              ><a
                class=" absolute inset-x-3 top-2.5 overflow-hidden overflow-ellipsis whitespace-nowrap"
                href={resolve(`/pipelines/${encodeURI(pipeline.name)}/`)}>{pipeline.name}</a
              ></td
            >
            <td
              class="{rowTd} relative w-12 border-surface-100-900 px-3 group-hover:bg-surface-50-950"
            >
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
            <td
              class="px-3 {rowTd} w-36 border-surface-100-900 text-center group-hover:bg-surface-50-950"
              ><PipelineStatus status={pipeline.status}></PipelineStatus></td
            >
            <td
              class="{rowTd} relative border-surface-100-900 whitespace-pre-wrap group-hover:bg-surface-50-950"
            >
              <span
                class="absolute inset-x-3 top-2.5 overflow-hidden align-middle overflow-ellipsis whitespace-nowrap"
              >
                {#if pipeline.deploymentError}
                  {@const message = pipeline.deploymentError.message}
                  <span class="fd fd-circle-alert pr-2 text-[20px] text-error-500"></span>
                  <Popover class="z-20" strategy="fixed">
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
            <td class="px-3 {rowTd} w-36 border-surface-100-900 group-hover:bg-surface-50-950"
              ><Tags pipelineName={pipeline.name} tags={pipeline.tags} {knownTags} {api}></Tags></td
            >
            <td class="{rowTd} relative border-surface-100-900 px-3 group-hover:bg-surface-50-950">
              <div class="flex w-full flex-nowrap items-center gap-2 text-nowrap">
                <PipelineVersion
                  pipelineName={pipeline.name}
                  runtimeVersion={pipeline.platformVersion}
                  baseRuntimeVersion={page.data.feldera!.version}
                  configuredRuntimeVersion={pipeline.programConfig.runtime_version}
                ></PipelineVersion>
              </div>
            </td>
            <td class="{rowTd} border-surface-100-900 px-3 group-hover:bg-surface-50-950">
              <div class="text-right text-nowrap">
                {pipeline.connectors?.numErrors ?? '-'}
              </div>
            </td>
            <td
              class="{rowTd} relative w-28 border-surface-100-900 px-3 group-hover:bg-surface-50-950"
            >
              <div class="w-32 text-right text-nowrap">
                {formatElapsedTime(pipeline.lastStatusSince, 'dhm')} ago
              </div>
            </td>
            <td
              class="{rowTd} relative w-40 border-surface-100-900 px-3 group-hover:bg-surface-50-950"
            >
              <div class="text-right text-nowrap">
                {pipeline.deploymentResourcesStatus === 'Provisioned'
                  ? formatDateTime(pipeline.deploymentResourcesStatusSince)
                  : ''}
              </div>
            </td>
          </tr>
        {:else}
          <tr>
            <td class="{td} border-b-[0.5px]"></td>
            <td class="{td} border-b-[0.5px] px-3" colspan={99}>No pipelines found</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>
