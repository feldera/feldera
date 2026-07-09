<script lang="ts">
  // Widget for kind K1 (per-worker magnitude scalar). Symmetric with every other kind widget:
  // takes a single `MetricGroup` and renders self-contained card body. The aggregated
  // MetricsDistributionBlock is a special case that packs many BarChartRow items into one shared
  // BarGrid; this widget is the single-metric case of the same system.
  import type { MetricGroup } from '../dispatch'
  import BarChartRow from './BarChartRow.svelte'
  import BarGrid from './BarGrid.svelte'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()
</script>

<BarGrid showHeader>
  <BarChartRow
    label={group.label}
    metricId={group.baseId}
    values={group.rows[0]?.values ?? []}
  />
</BarGrid>
