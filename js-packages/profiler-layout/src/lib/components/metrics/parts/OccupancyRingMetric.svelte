<script lang="ts">
  // K3 — per-worker fraction of capacity ({used, max} bytes). Rendered with the same percentage
  // bars as K2 (used/max as a percentage). A full cache is not inherently good or bad, so no
  // surface->error semantics apply (semantics='none', neutral surface color). The tooltip shows
  // the raw used / max byte values.
  import type { MetricGroup } from '../dispatch'
  import { numeric, rowEndingWith, workerCount } from './metricData'
  import PercentBars from './PercentBars.svelte'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  const used = $derived(rowEndingWith(group, '.used'))
  const max = $derived(rowEndingWith(group, '.max'))
  const n = $derived(workerCount(group))

  const values = $derived(
    Array.from({ length: n }, (_, i) => {
      const u = numeric(used?.values[i])
      const m = numeric(max?.values[i])
      return u !== undefined && m && m > 0 ? (u / m) * 100 : undefined
    })
  )

  const detail = (i: number) =>
    `${used?.values[i]?.toString() ?? '–'} / ${max?.values[i]?.toString() ?? '–'}`
</script>

<PercentBars {values} label={detail} />
