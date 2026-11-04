<script lang="ts">
  import { TableHandler } from '@vincjo/datatables'
  import { dateMax } from 'common-lib/date'
  import { formatDateTime } from 'common-lib/format'
  import { type NamesInUnion, unionName } from 'common-lib/union'
  import { Popover, Tooltip } from 'common-ui'
  import type { StatusTone } from 'common-ui/statusTone'
  import { errorTextClass } from 'common-ui/statusTone'
  import { fade } from 'svelte/transition'
  import { match } from 'ts-pattern'
  import Popup from '#lib/components/common/Popup.svelte'
  import PipelineStatus from '#lib/components/pipelines/list/PipelineStatus.svelte'
  import PipelineSearch from '#lib/components/pipelines/PipelineSearch.svelte'
  import ColumnResizer from '#lib/components/pipelines/table/ColumnResizer.svelte'
  import ThSort from '#lib/components/pipelines/table/ThSort.svelte'
  import { useElapsedTime } from '#lib/compositions/common/useElapsedTime.ts'
  import { useLayoutSettings } from '#lib/compositions/layout/useLayoutSettings.svelte.ts'
  import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
  import { statusToneColors } from '#lib/functions/pipelineStatusColor.ts'
  import { matchesPipelineSearch, nameMatchesFirst } from '#lib/functions/pipelines/search.ts'
  import { resolve } from '#lib/functions/svelte.ts'
  import type {
    PipelineStatus as PipelineStatusType,
    PipelineThumb
  } from '#lib/services/pipelineManager.ts'
  import type { Snippet } from '#lib/types/svelte.ts'
  import { page } from '$app/state'
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
    ['Failed', ['SystemError', 'SqlError', 'RustError']],
    ['Unavailable', ['Unavailable']]
  ]

  // The dot beside each status filter, in the colour its pipelines' status dots use.
  const filterDotTone: Record<string, StatusTone> = {
    Running: 'jade',
    Paused: 'blue',
    'Ready To Start': 'neutral',
    Compiling: 'warning',
    Failed: 'error',
    Unavailable: 'warning'
  }
  let statusFilterLabel = $state(filterStatuses[0][0])
  let statusMenuOpen = $state(false)

  let searchQuery = $state('')
  let tagFilters = $state<string[]>([])
  const pipelinesFiltered = $derived(
    pipelinesWithLastChange.filter((p) => matchesPipelineSearch(p, searchQuery, tagFilters))
  )
  // The column sort orders the rows inside each group.
  const rowsInDisplayOrder = $derived(nameMatchesFirst(table.rows, searchQuery))

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

  const { formatTimeAgo } = useElapsedTime()
  // Each row draws the line above it, the first one under the header. The table's own
  // border closes off the last row.
  const td = 'h-10 text-base border-t-[0.5px] border-line'
  const rowTd = td

  // Persist the active sort and restore it on the matching column. Each ThSort
  // owns the upstream `direction`/`onSort` API; the column identity lives here.
  const { pipelinesTableSort } = useLayoutSettings()
  const sortColumn = (column: string) => ({
    direction:
      pipelinesTableSort.value.column === column ? pipelinesTableSort.value.direction : undefined,
    onSort: (direction: 'asc' | 'desc') => (pipelinesTableSort.value = { column, direction })
  })

  // Column widths: the user can drag every column but the checkbox one, which holds a single
  // control. Message never has a set width in the layout: it takes whatever room the others
  // leave, so the table always spans the page, as wide as the controls above it. Its width,
  // `MESSAGE_MIN_WIDTH` until the user resizes it, is the least it takes; past that the table
  // outgrows the page and scrolls sideways.
  const { pipelinesTableColumnWidths } = useLayoutSettings()
  const defaultColumnWidths = {
    name: 240,
    storage: 80,
    status: 136,
    tags: 160,
    runtime: 112,
    errors: 96,
    statusChanged: 152,
    deployedOn: 192
  }
  type ResizableColumn = keyof typeof defaultColumnWidths | 'message'
  const CHECKBOX_WIDTH = 40
  const MESSAGE_MIN_WIDTH = 120
  const MIN_COLUMN_WIDTH = 64
  const messageWidth = $derived(pipelinesTableColumnWidths.value.message)
  // What Message currently fills, which a drag of its unsized handle starts from.
  let messageFilledWidth = $state(0)
  const columnWidth = (column: ResizableColumn) =>
    column === 'message'
      ? messageFilledWidth
      : (pipelinesTableColumnWidths.value[column] ?? defaultColumnWidths[column])
  const setColumnWidth = (column: ResizableColumn, width: number) =>
    (pipelinesTableColumnWidths.value = { ...pipelinesTableColumnWidths.value, [column]: width })
  const resetColumnWidth = (column: ResizableColumn) => {
    const { [column]: _, ...rest } = pipelinesTableColumnWidths.value
    pipelinesTableColumnWidths.value = rest
  }
  // The table's 1px border on each side counts towards its width, so it is added on top of
  // the columns' total rather than taken from them.
  const TABLE_BORDER_WIDTH = 2
  const tableMinWidth = $derived(
    TABLE_BORDER_WIDTH +
      CHECKBOX_WIDTH +
      (messageWidth ?? MESSAGE_MIN_WIDTH) +
      (Object.keys(defaultColumnWidths) as (keyof typeof defaultColumnWidths)[]).reduce(
        (sum, column) => sum + columnWidth(column),
        0
      )
  )
  // Header cells clip their label at the column's width and position its resize handle.
  const resizableTh = 'group/col relative h-10 overflow-hidden px-3'
</script>

{#snippet resizer(column: ResizableColumn, label: string)}
  <ColumnResizer
    width={columnWidth(column)}
    min={MIN_COLUMN_WIDTH}
    {label}
    onResize={(width) => setColumnWidth(column, width)}
    onReset={() => resetColumnWidth(column)}
  />
{/snippet}

<div class="pipeline-table-wrapper w-fit min-w-full">
  <!-- Opaque in the page's tint, so the rows scrolling under the controls stay hidden. -->
  <div class="sticky top-0 z-10 bg-page py-2" bind:clientHeight={controlsHeight}>
    <!-- The header and the controls share one wrapping row: the controls sit beside the
         header and drop to their own line only when they no longer fit next to it. -->
    <div
      class="sticky left-0 flex max-w-[100cqi] flex-wrap items-center gap-x-4 gap-y-2 px-2 md:px-8"
    >
      {#if header}
        {@render header()}
      {/if}
      <!-- The page reserves a scrollbar gutter that the header above it doesn't have; the
           negative margin carries the controls into it so they end where the header's
           buttons do. -->
      <div
        class="relative -mr-[var(--scroll-gutter-width)] ml-auto flex flex-row items-center gap-2"
      >
        <PipelineSearch
          {pipelines}
          bind:query={searchQuery}
          bind:tagFilters
          tagsLayout="above"
          inputClass="w-full sm:w-60"
        ></PipelineSearch>
        <Popup wrapperClass="sm:w-40" bind:isOpen={statusMenuOpen}>
          {#snippet trigger(toggle, isOpen)}
            <button
              type="button"
              data-testid="select-pipeline-status"
              class="input flex h-8 w-full items-center justify-between gap-2 rounded-[4px] text-left"
              aria-haspopup="listbox"
              aria-expanded={isOpen}
              onclick={toggle}
            >
              <span class="truncate">{statusFilterLabel}</span>
              <span
                class="fd fd-chevron-down shrink-0 text-[16px] text-surface-600-400 transition-transform {isOpen
                  ? 'rotate-180'
                  : ''}"
              ></span>
            </button>
          {/snippet}
          {#snippet content(close)}
            <div
              transition:fade={{ duration: 100 }}
              class="bg-white-dark absolute top-full left-0 z-30 mt-1 flex w-full min-w-[180px] flex-col overflow-hidden rounded p-1 shadow-md dark:border dark:border-surface-950-50/10"
              role="listbox"
              aria-label="Filter by status"
            >
              {#each filterStatuses as filter (filter[0])}
                {@const selected = filter[0] === statusFilterLabel}
                <button
                  type="button"
                  role="option"
                  aria-selected={selected}
                  class="flex h-9 items-center justify-between gap-2 rounded px-3 text-left hover:bg-row-hover"
                  onclick={() => {
                    statusFilterLabel = filter[0]
                    statusFilter.value = filter[0]
                    statusFilter.set()
                    close()
                  }}
                >
                  <span class="flex items-center gap-2">
                    {#if filterDotTone[filter[0]]}
                      <span
                        class="h-2 w-2 shrink-0 rounded-full {statusToneColors[
                          filterDotTone[filter[0]]
                        ].dot}"
                      ></span>
                    {:else}
                      <!-- Keeps "All Pipelines" in line with the labels that have a dot. -->
                      <span class="h-2 w-2 shrink-0" aria-hidden="true"></span>
                    {/if}
                    {filter[0]}
                  </span>
                  {#if selected}
                    <span class="fd fd-check text-[16px]"></span>
                  {/if}
                </button>
              {/each}
            </div>
          {/snippet}
        </Popup>
        {@render preHeaderEnd?.()}
      </div>
    </div>
  </div>
  <div class="flex px-2 md:pr-[calc(--spacing(8)-var(--scroll-gutter-width))] md:pl-8">
    <!-- The page wraps the table to fit its content, so a percentage width would have nothing
         to resolve against. As a flex item the table starts at the columns' total, which also
         sets how wide the page scrolls, and grows into any room left over, which Message takes. -->
    <table
      class="bg-white-dark flex-1 border-separate border-spacing-0 rounded-lg border border-line [&_th:first-child]:rounded-tl-lg [&_th:last-child]:rounded-tr-lg [&_tr:last-child>td:first-child]:rounded-bl-lg [&_tr:last-child>td:last-child]:rounded-br-lg"
      style="table-layout: fixed; flex-basis: {tableMinWidth}px; min-width: {tableMinWidth}px"
    >
      <colgroup>
        <col style="width: {CHECKBOX_WIDTH}px" />
        <col style="width: {columnWidth('name')}px" />
        <col style="width: {columnWidth('storage')}px" />
        <col style="width: {columnWidth('status')}px" />
        <col />
        <col style="width: {columnWidth('tags')}px" />
        <col style="width: {columnWidth('runtime')}px" />
        <col style="width: {columnWidth('errors')}px" />
        <col style="width: {columnWidth('statusChanged')}px" />
        <col style="width: {columnWidth('deployedOn')}px" />
      </colgroup>
      <thead class="bg-white-dark sticky" style="top: {controlsHeight}px; z-index: 1;">
        <tr>
          <th class="px-3 text-left"
            ><div class="flex h-full items-center">
              <!-- A 24px click area around the 16px box. -->
              <label class="-m-1 flex h-6 w-6 cursor-pointer items-center justify-center">
                <input
                  class="checkbox"
                  type="checkbox"
                  aria-label="Select all pipelines"
                  checked={table.isAllSelected}
                  onclick={() => table.selectAll()}
                />
              </label>
            </div></th
          >
          <ThSort class={resizableTh} {table} field="name" {...sortColumn('name')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Pipeline name</span
            >{@render resizer('name', 'Pipeline name')}</ThSort
          >
          <!-- Sort from storage in use, through clearing, to cleared. -->
          <ThSort
            {table}
            class="{resizableTh} justify-center"
            field={(p) =>
              match(p.storageStatus)
                .with('InUse', () => 0)
                .with('Clearing', () => 1)
                .with('Cleared', () => 2)
                .exhaustive()}
            {...sortColumn('storage')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Storage</span
            >{@render resizer('storage', 'Storage')}</ThSort
          >
          <ThSort {table} class={resizableTh} field="status" {...sortColumn('status')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Status</span
            >{@render resizer('status', 'Status')}</ThSort
          >
          <th class="{resizableTh} text-left" bind:offsetWidth={messageFilledWidth}
            ><span
              class="block w-0 min-w-full truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Message</span
            >{@render resizer('message', 'Message')}</th
          >
          <!-- Sort by the tags in alphabetical order, so pipelines sharing a tag group together
               whatever order their tags were added in. -->
          <ThSort
            {table}
            class={resizableTh}
            field={(p) => [...p.tags].sort().join(', ')}
            {...sortColumn('tags')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Tags</span
            >{@render resizer('tags', 'Tags')}</ThSort
          >
          <ThSort
            {table}
            class={resizableTh}
            field="platformVersion"
            {...sortColumn('platformVersion')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Runtime</span
            >{@render resizer('runtime', 'Runtime')}</ThSort
          >
          <ThSort
            {table}
            class={resizableTh}
            field={(p) => p.connectors?.numErrors}
            {...sortColumn('numErrors')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Errors</span
            >{@render resizer('errors', 'Errors')}</ThSort
          >
          <ThSort
            {table}
            class={resizableTh}
            field="lastStatusSince"
            {...sortColumn('lastStatusSince')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Status changed</span
            >{@render resizer('statusChanged', 'Status changed')}</ThSort
          >
          <ThSort
            {table}
            class={resizableTh}
            field="deploymentResourcesStatusSince"
            {...sortColumn('deploymentResourcesStatusSince')}
            ><span
              class="truncate font-dm-mono text-sm font-normal tracking-[0.1em] text-surface-600-400 uppercase"
              >Deployed on</span
            >{@render resizer('deployedOn', 'Deployed on')}</ThSort
          >
        </tr>
      </thead>
      <tbody>
        {#each rowsInDisplayOrder as pipeline}
          <!-- A click anywhere on the row that isn't on a link or control toggles its
               selection, like clicking its checkbox. -->
          <tr
            class="group cursor-pointer"
            data-testid="box-row-{pipeline.name}"
            onclick={(e) => {
              if (
                (e.target as HTMLElement).closest(
                  'a, button, input, label, select, textarea, [role="button"], [role="dialog"], [role="tooltip"]'
                )
              ) {
                return
              }
              table.select(pipeline.name)
            }}
            ><td class="{rowTd} px-3 group-hover:bg-row-hover">
              <div class="flex h-full items-center">
                <label class="-m-1 flex h-6 w-6 cursor-pointer items-center justify-center">
                  <input
                    class="checkbox"
                    type="checkbox"
                    aria-label="Select {pipeline.name}"
                    checked={table.selected.includes(pipeline.name)}
                    onclick={() => table.select(pipeline.name)}
                  />
                </label>
              </div>
            </td>
            <td class="{rowTd} relative group-hover:bg-row-hover"
              ><a
                class=" absolute inset-x-3 top-2.5 overflow-hidden overflow-ellipsis whitespace-nowrap"
                href={resolve(`/pipelines/${encodeURI(pipeline.name)}/`)}>{pipeline.name}</a
              ></td
            >
            <td class="{rowTd} relative px-3 group-hover:bg-row-hover">
              <div
                class="fd {pipeline.storageStatus === 'Cleared'
                  ? 'fd-database-off text-surface-500'
                  : 'fd-database'} text-center text-[16px]"
              ></div>
              <Tooltip
                >{match(pipeline.storageStatus)
                  .with('InUse', () => 'Storage in use')
                  .with('Clearing', () => 'Clearing storage')
                  .with('Cleared', () => 'Storage cleared')
                  .exhaustive()}</Tooltip
              >
            </td>
            <td class="px-3 {rowTd} text-left group-hover:bg-row-hover"
              ><div class="overflow-hidden">
                <PipelineStatus status={pipeline.status}></PipelineStatus>
              </div></td
            >
            <td class="{rowTd} relative whitespace-pre-wrap group-hover:bg-row-hover">
              {#if pipeline.deploymentError}
                {@const message = pipeline.deploymentError.message}
                <!-- The Popover attaches to its preceding sibling, so hovering the icon or the
                     text opens the full message. -->
                <span
                  class="absolute inset-x-3 top-1/2 flex -translate-y-1/2 items-center gap-2 text-sm whitespace-nowrap"
                >
                  <span class="fd fd-circle-alert shrink-0 text-[16px] {errorTextClass}"></span>
                  <span class="truncate">
                    {message.slice(
                      0,
                      ((idx) => (idx > 0 ? idx : undefined))(message.indexOf('\n'))
                    )}
                  </span>
                </span>
                <!-- Only the trimmed message keeps its line breaks, so the template's own
                     whitespace doesn't show as blank lines. -->
                <Popover class="z-20 p-1! whitespace-normal" strategy="fixed">
                  <div
                    class="scrollbar max-h-[50vh] max-w-[min(420px,80vw)] overflow-auto text-sm leading-5"
                  >
                    <span class="whitespace-pre-wrap">{message.trim()}</span>
                  </div>
                </Popover>
              {/if}
            </td>
            <td class="px-3 {rowTd} group-hover:bg-row-hover"
              ><Tags pipelineName={pipeline.name} tags={pipeline.tags} {knownTags} {api}></Tags></td
            >
            <td class="{rowTd} relative px-3 group-hover:bg-row-hover">
              <div class="flex w-full flex-nowrap items-center gap-2 overflow-hidden text-nowrap">
                <PipelineVersion
                  pipelineName={pipeline.name}
                  runtimeVersion={pipeline.platformVersion}
                  baseRuntimeVersion={page.data.feldera!.version}
                  configuredRuntimeVersion={pipeline.programConfig.runtime_version}
                ></PipelineVersion>
              </div>
            </td>
            <td class="{rowTd} px-3 group-hover:bg-row-hover">
              <div class="truncate">
                {pipeline.connectors?.numErrors ?? '-'}
              </div>
            </td>
            <td class="{rowTd} relative px-3 group-hover:bg-row-hover">
              {#if !isNaN(pipeline.lastStatusSince.valueOf())}
                <time class="block truncate" datetime={pipeline.lastStatusSince.toISOString()}>
                  {formatTimeAgo(pipeline.lastStatusSince)}
                </time>
                <Tooltip>{formatDateTime(pipeline.lastStatusSince)}</Tooltip>
              {:else}
                —
              {/if}
            </td>
            <td class="{rowTd} relative px-3 group-hover:bg-row-hover">
              <div class="truncate">
                {pipeline.deploymentResourcesStatus === 'Provisioned'
                  ? formatDateTime(pipeline.deploymentResourcesStatusSince)
                  : ''}
              </div>
            </td>
          </tr>
        {:else}
          <tr>
            <td class={td}></td>
            <td class="{td} px-3" colspan={99}>No pipelines found</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>
