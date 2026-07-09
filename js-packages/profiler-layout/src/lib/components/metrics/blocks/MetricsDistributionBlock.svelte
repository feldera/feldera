<script lang="ts">
  // Special case of the generic metric system: aggregates many bar-chart metrics into one card
  // with a single shared Avg/Min/Max header and aligned columns. Built from the same BarGrid +
  // BarChartRow atoms as the single-metric BarChartMetric widget. Non-bar kinds render as their
  // own cards via MetricKindBlock and never reach here (see `isCardKind` / MetricsView).
  import type { PropertyValue } from 'profiler-lib'
  import type { MetricGroup } from '../dispatch'
  import BarChartRow from '../parts/BarChartRow.svelte'
  import BarGrid from '../parts/BarGrid.svelte'

  interface Props {
    id: string
    title?: string
    entries: MetricGroup[]
  }
  const { id, title, entries }: Props = $props()

  // A K1 scalar is one bar row; any other (not-yet-carded) kind still routed here renders one
  // bar per decoded sub-row, its legacy look.
  type BarRow = { key: string; label: string; metricId: string; values: PropertyValue[] }
  function barRows(group: MetricGroup): BarRow[] {
    if (group.kind === 'K1') {
      return [
        {
          key: group.baseId,
          label: group.label,
          metricId: group.baseId,
          values: group.rows[0]?.values ?? []
        }
      ]
    }
    return group.rows.map((r) => ({
      key: group.baseId + r.suffix,
      label: r.label,
      metricId: group.baseId + r.suffix,
      values: r.values
    }))
  }
</script>

<div class="metrics-block rounded-container bg-white-dark px-4 py-2 shadow-sm" data-block-id={id}>
  <div class="scrollbar overflow-x-auto overflow-y-visible">
    <BarGrid showHeader {title}>
      {#each entries as entry (entry.baseId)}
        {#each barRows(entry) as br (br.key)}
          <BarChartRow label={br.label} metricId={br.metricId} values={br.values} />
        {/each}
      {/each}
    </BarGrid>
  </div>
</div>
