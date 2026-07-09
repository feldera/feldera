<script lang="ts">
  // Per-worker percentage bars, drawn with plain divs (like the distribution bars), spanning the
  // full width. Each bar has a fixed total height; the fill grows from the top and bottom edges
  // inward — two stripes that are single-px lines at 0% and meet in the middle at 100%. A bar can
  // be clicked to select its worker (when `onselect` is provided); the selected bar is outlined.
  //
  // Color carries semantics: the fill interpolates between a theme-adaptive neutral
  // (surface-100-900) and error-500 by a "badness" fraction. `semantics` says which end is bad:
  //   - 'low-bad':  low % is bad (e.g. cache hit rate) — redder as the value drops.
  //   - 'high-bad': high % is bad — redder as the value rises.
  //   - 'none':     no known semantics — always the neutral surface color.
  import { Tooltip } from 'common-ui'
  import { mixRgb, neutralErrorScale } from '../../../functions/format'

  interface Props {
    /** Percentage 0..100 per worker; undefined renders an empty bar. */
    values: (number | undefined)[]
    height?: number
    /** Which end of the range is "bad" (drives the surface->error interpolation). */
    semantics?: 'low-bad' | 'high-bad' | 'none'
    selected?: number | null
    onselect?: (worker: number) => void
    format?: (n: number) => string
    /** Optional per-worker tooltip text; defaults to the formatted percentage. */
    label?: (worker: number) => string
  }
  const {
    values,
    height = 24,
    semantics = 'none',
    selected = null,
    onselect,
    format = (n) => `${Math.round(n)}%`,
    label
  }: Props = $props()

  const scale = $derived(neutralErrorScale())

  // Height of each stripe (top and bottom): a 2px line at 0%, growing to half the bar at 100%.
  function stripe(v: number | undefined): number {
    if (v === undefined) {
      return 0
    }
    const frac = Math.max(0, Math.min(1, v / 100))
    return Math.max(2, frac * (height / 2))
  }

  // Fill color: neutral surface with no semantics, otherwise blended toward error by badness.
  function color(v: number | undefined): string {
    if (v === undefined || semantics === 'none') {
      return mixRgb(scale.neutral, scale.error, 0)
    }
    const frac = Math.max(0, Math.min(1, v / 100))
    const badness = semantics === 'low-bad' ? 1 - frac : frac
    return mixRgb(scale.neutral, scale.error, badness)
  }
</script>

<div class="flex items-stretch gap-0.5" style:height="{height}px">
  {#each values as v, i (i)}
    {@const s = stripe(v)}
    {@const c = color(v)}
    <button
      type="button"
      class="relative min-w-0 flex-1 rounded-sm"
      class:selected={selected === i}
      onclick={() => onselect?.(i)}
      aria-label={`Worker ${i}`}
    >
      <div
        class="absolute inset-x-0 top-0 rounded-t-sm"
        style:height="{s}px"
        style:background-color={c}
      ></div>
      <div
        class="absolute inset-x-0 bottom-0 rounded-b-sm"
        style:height="{s}px"
        style:background-color={c}
      ></div>
    </button>
    <Tooltip class="whitespace-nowrap" placement="top">
      Worker {i}: {label ? label(i) : v === undefined ? '–' : format(v)}
    </Tooltip>
  {/each}
</div>

<style>
  .selected {
    outline: 2px solid var(--color-primary-500);
    outline-offset: 1px;
  }
</style>
