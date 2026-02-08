<script lang="ts" module>
  import type { NodeAttributes, TooltipRow } from 'profiler-lib'
  import { measurementCategory, measurementDescription, shadeOfRed } from 'profiler-lib'
  import { SvelteSet } from 'svelte/reactivity'
  import { groupBy } from '$lib/functions/array'

  export type TooltipData =
    | { nodeAttributes: NodeAttributes }
    | {
        genericTable: {
          header: string
          columns: string[]
          rows: {
            stub: { text: string; onclick?: () => void }
            cells: {
              text: string
              operation: string
              normalizedValue: number
            }[]
          }[]
        }
      }

  // Track which metric categories are collapsed (true = collapsed, false = expanded)
  let collapsedCategories = $state(new SvelteSet<string>())
  // If true show the advanced categories
  let showAdvanced = $state(false)

  // Toggle category collapse state
  function toggleCategory(category: string) {
    if (collapsedCategories.has(category)) {
      collapsedCategories.delete(category)
    } else {
      collapsedCategories.add(category)
    }
  }
</script>

<script lang="ts">
    let { value, sticky }: { value: TooltipData | null; sticky: boolean } =
        $props();

    let containerRef: HTMLElement | undefined = $state();

    export function resetScroll() {
        if (containerRef) {
            containerRef.scrollTop = 0;
        }
    }
</script>

{#if value}
    <div
        class="profiler-tooltip-container {sticky ? '' : 'pointer-events-none'}"
        bind:this={containerRef}
    >
        <div class="profiler-tooltip">
            {#if "nodeAttributes" in value}
                {@const { nodeAttributes } = value}
                <table>
                    <!-- Header row with worker names -->
                    <thead>
                        <tr>
                            <th
                                ><label
                                    ><input
                                        type="checkbox"
                                        id="show-advanced"
                                        bind:checked={showAdvanced}
                                    /> show advanced</label
                                ></th
                            >
                            {#each nodeAttributes.columns as column}
                                <th>{column}</th>
                            {/each}
                        </tr>
                    </thead>

                    <!-- Metric rows grouped by category -->
                    <tbody>
                        {#each groupBy( nodeAttributes.rows, (row: TooltipRow) => measurementCategory(row.metric), ) as [category, rows]}
                            <!-- Category header row -->
                            <tr
                                class="category-header"
                                onclick={() => toggleCategory(category)}
                                onkeydown={(e) =>
                                    (e.key === "Enter" || e.key === " ") &&
                                    toggleCategory(category)}
                                role="button"
                                tabindex="0"
                            >
                                <td
                                    colspan={nodeAttributes.columns.length + 1}
                                    class="category-header-cell"
                                >
                                    <span class="category-icon font-mono"
                                        >{collapsedCategories.has(category)
                                            ? "▶"
                                            : "▼"}</span
                                    >
                                    <span class="">{category || "Other"}</span>
                                </td>
                            </tr>
                            <!-- Metric rows for this category -->
                            {#if !collapsedCategories.has(category)}
                                {#each rows as row}
                                    {#if !measurementDescription(row.metric).advanced || showAdvanced}
                                        <tr>
                                            <td
                                                class:current-metric={row.isCurrentMetric}
                                                title={measurementDescription(
                                                    row.metric,
                                                ).description}>{row.metric}</td
                                            >
                                            {#each row.cells as cell}
                                                {@const percent =
                                                    cell.percentile}
                                                <td
                                                    style:background-color={shadeOfRed(
                                                        percent,
                                                    )}
                                                    style:color="black"
                                                    class="text-right"
                                                >
                                                    {cell.value}
                                                </td>
                                            {/each}
                                        </tr>
                                    {/if}
                                {/each}
                            {/if}
                        {/each}

                        <!-- Source code row -->
                        {#if nodeAttributes.sources}
                            <tr>
                                <td>sources</td>
                                <td
                                    colspan={nodeAttributes.columns.length}
                                    class="source-code"
                                    >{nodeAttributes.sources}</td
                                >
                            </tr>
                        {/if}

                        <!-- Additional attributes -->
                        {#each Array.from(nodeAttributes.attributes.entries()) as [key, value]}
                            <tr>
                                <td class="whitespace-nowrap">{key}</td>
                                <td
                                    colspan={nodeAttributes.columns.length}
                                    class="whitespace-nowrap">{value}</td
                                >
                            </tr>
                        {/each}
                    </tbody>
                </table>
            {:else if "genericTable" in value}
                {@const { genericTable } = value}
                <table>
                    <!-- Header row with worker names -->
                    <thead>
                        <tr>
                            <th colspan={Number.MAX_SAFE_INTEGER}
                                >{genericTable.header}</th
                            >
                        </tr>
                        <tr>
                            {#each genericTable.columns as column}
                                <th>{column}</th>
                            {/each}
                        </tr>
                    </thead>

                    <!-- Metric rows -->
                    <tbody>
                        {#each genericTable.rows as row}
                            <tr>
                                <td
                                    onclick={() => {
                                        row.stub.onclick?.();
                                    }}
                                    class={row.stub.onclick
                                        ? "cursor-pointer"
                                        : ""}>{row.stub.text}</td
                                >
                                {#each row.cells as cell}
                                    {@const percent = cell.normalizedValue}
                                    <td
                                        style:background-color={shadeOfRed(
                                            percent,
                                        )}
                                        style:color="black"
                                        class="text-right"
                                    >
                                        {cell.text}
                                    </td>
                                    <td>{cell.operation}</td>
                                {/each}
                            </tr>
                        {/each}
                    </tbody>
                </table>
            {/if}
        </div>
    </div>
{/if}

<style>
    .profiler-tooltip-container {
        position: absolute;
        top: 0.5rem;
        right: 0.5rem;
        z-index: 2;
        border-radius: 8px;
        max-height: calc(100% - 1rem);
        max-width: calc(100% - 1rem);
        overflow-y: auto;
        overflow-x: auto;
    }
    /* Tooltip styling */
    .profiler-tooltip {
        background-color: black;
        padding: 0;
    }

    .profiler-tooltip table {
        background-color: transparent;
        border-collapse: collapse;
        font-size: 12px;
        width: 100%;
    }

    .profiler-tooltip table thead {
        position: sticky;
        top: 0;
        z-index: 1;
    }

    .profiler-tooltip table td,
    .profiler-tooltip table th {
        padding: 2px 10px;
        color: white;
        white-space: nowrap;
    }

    .profiler-tooltip table th {
        background-color: rgb(102, 126, 234);
        font-weight: 600;
        text-align: center;
    }

    .profiler-tooltip table td {
        background-color: black;
    }

    .profiler-tooltip table td.current-metric {
        background-color: blue;
    }

    .profiler-tooltip table td.source-code {
        font-family: monospace;
        white-space: pre-wrap;
        text-align: left;
        min-width: 80ch;
    }

    /* Category header styling */
    .profiler-tooltip table tr.category-header {
        cursor: pointer;
        user-select: none;
        position: sticky;
        top: 24px;
        z-index: 1;
    }

    .profiler-tooltip table tr.category-header:hover {
        opacity: 0.9;
    }

    .profiler-tooltip table td.category-header-cell {
        background-color: rgb(102, 126, 234);
    }

    .profiler-tooltip table .category-icon {
        display: inline-block;
        width: 1em;
        margin-right: 0.5em;
        font-size: 16px;
    }

    /* Scrollbar styling */
    .profiler-tooltip-container::-webkit-scrollbar {
        width: 8px;
    }

    .profiler-tooltip-container::-webkit-scrollbar-track {
        background: rgba(0, 0, 0, 0.1);
        border-radius: 4px;
    }

    .profiler-tooltip-container::-webkit-scrollbar-thumb {
        background: rgba(102, 126, 234, 0.5);
        border-radius: 4px;
    }

    .profiler-tooltip-container::-webkit-scrollbar-thumb:hover {
        background: rgba(102, 126, 234, 0.7);
    }
</style>
