<script lang="ts">
  // K2 — per-worker bounded fraction (percent). A full-width row of per-worker bars whose fill
  // grows from the top and bottom edges inward (PercentBars); no ECharts.
  import type { MetricGroup } from '../dispatch'
  import { numeric } from './metricData'
  import PercentBars from './PercentBars.svelte'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  const values = $derived((group.rows[0]?.values ?? []).map((v) => numeric(v)))
</script>

<PercentBars {values} />
