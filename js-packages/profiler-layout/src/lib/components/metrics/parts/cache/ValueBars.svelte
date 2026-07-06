<script lang="ts">
  // Per-worker value bars reusing the K1 bar look (log-scaled heights via `barMetrics`, shared
  // 6px..24px envelope, `barColor`), rendered through the shared BarRow. A bar can be clicked to
  // select its worker across the whole tile; the selected bar gets a primary outline.
  import { barColor, barMetrics } from '../../colors'
  import BarRow from '../BarRow.svelte'

  interface Props {
    /** One value per worker; undefined entries render as a zero-height gap. */
    values: (number | undefined)[]
    format?: (n: number) => string
    selected?: number | null
    onselect?: (worker: number) => void
  }
  const { values, format = (n) => `${n}`, selected = null, onselect }: Props = $props()

  // Simple value bars use a fixed 6px..24px envelope throughout (see colors.barMetrics).
  const MIN_H = 6
  const MAX_H = 24

  const stats = $derived.by(() => {
    const nums = values.filter((v): v is number => v !== undefined)
    if (nums.length === 0) {
      return { min: 0, max: 0 }
    }
    return { min: Math.min(...nums), max: Math.max(...nums) }
  })

  const bars = $derived(
    values.map((v, i) => {
      if (v === undefined) {
        return { height: 0, color: barColor(0), tooltip: `Worker ${i}: –` }
      }
      const { t, height } = barMetrics(v, stats.min, stats.max, MIN_H, MAX_H)
      return { height, color: barColor(t), tooltip: `Worker ${i}: ${format(v)}` }
    })
  )
</script>

<BarRow {bars} height={MAX_H} {selected} {onselect} />
