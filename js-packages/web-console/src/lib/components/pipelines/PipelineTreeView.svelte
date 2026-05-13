<script lang="ts">
  import { match } from 'ts-pattern'
  import { page } from '$app/state'
  import { Popover } from '$lib/components/common/Popover.svelte'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import DoubleClickInput from '$lib/components/input/DoubleClickInput.svelte'
  import DraggableTreeView from '$lib/components/pipelines/DraggableTreeView.svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import PipelineVersion from '$lib/components/pipelines/table/PipelineVersion.svelte'
  import { formatDateTime, useElapsedTime } from '$lib/functions/format'
  import type { PipelineThumb } from '$lib/services/pipelineManager'

  // A pipeline row enriched with the derived `lastStatusSince` column.
  type Row = PipelineThumb & { lastStatusSince: Date }

  let {
    items,
    getFolderPath,
    selected = $bindable(),
    onMoveToFolder,
    onCreateFolderFor,
    onMoveFolder,
    stickyTop
  }: {
    items: Row[]
    getFolderPath: (item: Row) => string
    selected: string[]
    onMoveToFolder: (pipelineName: string, folderPath: string) => void
    onCreateFolderFor: (aName: string, bName: string, newFolderPath: string) => void
    onMoveFolder: (oldPath: string, newPath: string) => void
    /** Pixel offset for the sticky header row, accounting for any controls
        bar that sits above it in the scroll container. */
    stickyTop: number
  } = $props()

  const { formatElapsedTime } = useElapsedTime()

  const allSelected = $derived(items.length > 0 && selected.length === items.length)
  const someSelected = $derived(selected.length > 0 && selected.length < items.length)
</script>

<div class="pipeline-table">
  <DraggableTreeView
    {items}
    {getFolderPath}
    bind:selected
    {onMoveToFolder}
    {onCreateFolderFor}
    {onMoveFolder}
    rowClass="pipeline-row"
    containerClass="pipeline-grid md:px-6"
    dropTargetClass="bg-primary-50-950/50"
    dndContainer="pipeline-tree"
    defaultExpanded={false}
  >
    {#snippet header()}
      <div
        class="pipeline-row pipeline-row--header bg-white-dark"
        style="position: sticky; top: {stickyTop}px; z-index: 1;"
      >
        <div class="pipeline-cell px-2">
          <div class="flex items-center gap-2">
            <span class="w-5" aria-hidden="true"></span>
            <input
              class="checkbox"
              type="checkbox"
              checked={allSelected}
              indeterminate={someSelected}
              onclick={() => {
                selected = allSelected ? [] : items.map((p) => p.name)
              }}
            />
          </div>
        </div>
        <div class="pipeline-cell pipeline-cell--header">Pipeline name</div>
        <div class="pipeline-cell pipeline-cell--header">Storage</div>
        <div class="pipeline-cell pipeline-cell--header">
          <span class="ml-8">Status</span>
        </div>
        <div class="pipeline-cell pipeline-cell--header">Message</div>
        <div class="pipeline-cell pipeline-cell--header text-nowrap">
          <span class="inline xl:hidden">Runtime</span>
          <span class="hidden xl:!inline">Runtime version</span>
        </div>
        <div class="pipeline-cell pipeline-cell--header text-nowrap">Status changed</div>
        <div class="pipeline-cell pipeline-cell--header text-center text-nowrap">Deployed on</div>
        <div class="pipeline-cell pipeline-cell--header pr-4 text-right">
          <span class="inline xl:hidden">Errors</span>
          <span class="hidden text-nowrap xl:!inline">Runtime errors</span>
        </div>
      </div>
    {/snippet}

    {#snippet folderRow(node, ctx)}
      <div class="pipeline-cell px-2">
        <div class="flex items-center gap-2">
          <span
            data-tree-drag-handle
            class="flex w-5 cursor-grab items-center justify-center text-[18px] leading-none text-surface-400 select-none hover:text-surface-700-300 active:cursor-grabbing"
            style="font-family: monospace;"
            title="Drag to move folder"
            aria-label="Drag handle"
            role="button"
            tabindex="-1">⋮⋮</span
          >
          <input
            class="checkbox"
            type="checkbox"
            checked={ctx.checkState === 'all'}
            indeterminate={ctx.checkState === 'some'}
            onchange={() => ctx.toggleSelection()}
          />
        </div>
      </div>
      <div class="pipeline-cell pipeline-cell--folder-name">
        <div
          class="flex flex-nowrap items-center gap-2"
          style="padding-left: {ctx.depth * 1.25}rem"
        >
          <button
            class="fd {ctx.isExpanded ? 'fd-chevron-down' : 'fd-chevron-right'} text-[18px]"
            aria-label={ctx.isExpanded ? 'Collapse folder' : 'Expand folder'}
            onclick={() => ctx.toggleExpanded()}
          ></button>
          <DoubleClickInput
            value={node.name}
            onvalue={(name) => ctx.renameFolder(name)}
            class="font-medium"
            inputClass="input py-0 pl-1 -ml-1 h-6 text-base w-fit"
          >
            {#snippet children()}
              <!-- svelte-ignore a11y_click_events_have_key_events -->
              <!-- svelte-ignore a11y_no_static_element_interactions -->
              <span onclick={() => ctx.toggleExpanded()} class="font-medium"
                >{node.name || '(root)'}</span
              >
            {/snippet}
          </DoubleClickInput>
          <span class="text-surface-500">({ctx.leafCount})</span>
        </div>
      </div>
    {/snippet}

    {#snippet leafRow(pipeline)}
      <div class="pipeline-cell px-2">
        <div class="flex items-center gap-2">
          <span
            data-tree-drag-handle
            class="flex w-5 cursor-grab items-center justify-center text-[18px] leading-none text-surface-400 select-none hover:text-surface-700-300 active:cursor-grabbing"
            style="font-family: monospace;"
            title="Drag to move pipeline"
            aria-label="Drag handle"
            role="button"
            tabindex="-1">⋮⋮</span
          >
          <input
            class="checkbox"
            type="checkbox"
            checked={selected.includes(pipeline.name)}
            onclick={() => {
              selected = selected.includes(pipeline.name)
                ? selected.filter((n) => n !== pipeline.name)
                : [...selected, pipeline.name]
            }}
          />
        </div>
      </div>
      <div class="pipeline-cell">
        <a
          class="block w-full overflow-hidden overflow-ellipsis whitespace-nowrap"
          style="padding-left: var(--tree-indent, 0)"
          href="/pipelines/{pipeline.name}/">{pipeline.name}</a
        >
      </div>
      <div class="pipeline-cell relative">
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
      </div>
      <div class="pipeline-cell pr-2">
        <PipelineStatus status={pipeline.status}></PipelineStatus>
      </div>
      <div class="pipeline-cell whitespace-pre-wrap">
        <span class="block overflow-hidden overflow-ellipsis whitespace-nowrap">
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
      </div>
      <div class="pipeline-cell">
        <div class="flex w-full flex-nowrap items-center gap-2 text-nowrap">
          <PipelineVersion
            pipelineName={pipeline.name}
            runtimeVersion={pipeline.platformVersion}
            baseRuntimeVersion={page.data.feldera!.version}
            configuredRuntimeVersion={pipeline.programConfig.runtime_version}
          ></PipelineVersion>
        </div>
      </div>
      <div class="pipeline-cell">
        <div class="text-right text-nowrap">
          {formatElapsedTime(pipeline.lastStatusSince, 'dhm')} ago
        </div>
      </div>
      <div class="pipeline-cell">
        <div class="pr-1 text-right text-nowrap">
          {pipeline.deploymentResourcesStatus === 'Provisioned'
            ? formatDateTime(pipeline.deploymentResourcesStatusSince)
            : ''}
        </div>
      </div>
      <div class="pipeline-cell pr-4">
        <div class="text-right text-nowrap">
          {pipeline.connectors?.numErrors ?? '-'}
        </div>
      </div>
    {/snippet}
  </DraggableTreeView>
</div>

<style>
  .pipeline-table {
    width: fit-content;
    min-width: 100%;
  }

  /* Grid template:
     checkbox+grip | name | storage | status | message | errors | version | status-changed | deployed-on
     `errors` and `status-changed` use `max-content` so the column track grows
     to fit the header label (which is longer than any cell value); the rest
     stay fixed/flexible to keep the message column elastic. */
  /* On xl screens the version column header switches to "Runtime version", so
     widen only that fixed-width column; errors/status-changed remain
     `max-content` and re-measure their (now longer) header text automatically. */
  .pipeline-table {
    --pipeline-grid: 4.25rem minmax(180px, 3fr) 3rem 9rem minmax(180px, 3fr) 6rem max-content 11rem
      max-content;
  }
  @media (min-width: 1280px) {
    .pipeline-table {
      --pipeline-grid: 4.25rem minmax(180px, 3fr) 3rem 9rem minmax(180px, 3fr) 8rem max-content
        11rem max-content;
    }
  }
  /* The container itself is the grid, and rows opt into the same column
     tracks with `subgrid`. This means `max-content` columns measure across
     all rows + header instead of resizing per-row, so columns line up. */
  :global(.pipeline-grid) {
    display: grid;
    grid-template-columns: var(--pipeline-grid);
    background: inherit;
  }
  :global(.pipeline-row) {
    display: grid;
    grid-template-columns: subgrid;
    grid-column: 1 / -1;
    align-items: center;
    border-top: 0.5px solid rgb(0 0 0 / 0.08);
    transition: background-color 80ms ease;
    min-height: 2.25rem;
    position: relative;
  }
  :global(.dark .pipeline-row) {
    border-top-color: rgb(255 255 255 / 0.08);
  }
  :global(.pipeline-row:not(.pipeline-row--header):hover) {
    background-color: var(--color-surface-50);
  }
  :global(.dark .pipeline-row:not(.pipeline-row--header):hover) {
    background-color: var(--color-surface-950);
  }
  :global(.pipeline-row--header) {
    font-weight: 400;
  }
  /* The folder-name cell spans from column 2 to the end of the subgrid so
     the folder name (and its rename input) occupies all remaining columns.
     Folder rows carry `tree-row--folder` (from DraggableTreeView), not
     `pipeline-row--folder`, so the selector targets that class. */
  :global(.tree-row--folder > .pipeline-cell--folder-name) {
    grid-column: 2 / -1;
  }
  :global(.pipeline-cell) {
    padding: 0.25rem 0.25rem;
    min-width: 0;
    overflow: visible;
  }
  :global(.pipeline-cell--header) {
    padding: 0.25rem 0.25rem;
    font-size: 0.875rem;
  }
</style>
