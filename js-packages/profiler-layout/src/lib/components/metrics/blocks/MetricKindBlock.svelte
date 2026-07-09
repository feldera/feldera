<script lang="ts">
  // Generic single-metric card: dispatches a MetricGroup to the widget for its kind. Every kind
  // is symmetric here — including K1 (bar chart), which renders via the BarChartMetric widget.
  // MetricsDistributionBlock is the special case that aggregates many K1 metrics into one card;
  // this block is the single-metric case of the same system. See ../README.md.
  import { Tooltip } from 'common-ui'
  import CircleHelp from '../../icons/CircleHelp.svelte'
  import type { MetricGroup } from '../dispatch'
  import { kindHelp } from '../kindHelp'
  import BarChartMetric from '../parts/BarChartMetric.svelte'
  import BooleanMetric from '../parts/BooleanMetric.svelte'
  import CacheCountsMetric from '../parts/CacheCountsMetric.svelte'
  import ChipMetric from '../parts/ChipMetric.svelte'
  import CompactionStateMetric from '../parts/CompactionStateMetric.svelte'
  import KeyDistributionMatrix from '../parts/KeyDistributionMatrix.svelte'
  import MergesHistogram from '../parts/MergesHistogram.svelte'
  import OccupancyRingMetric from '../parts/OccupancyRingMetric.svelte'
  import PercentRingMetric from '../parts/PercentRingMetric.svelte'
  import SizeHistogram from '../parts/SizeHistogram.svelte'
  import StatsMetric from '../parts/StatsMetric.svelte'

  interface Props {
    id: string
    group: MetricGroup
  }
  const { id, group }: Props = $props()

  const help = $derived(kindHelp(group.kind))
</script>

<div class="metrics-block rounded-container bg-white-dark px-4 py-2 shadow-sm" data-block-id={id}>
  {#if group.kind === 'K1'}
    <div class="scrollbar overflow-x-auto overflow-y-visible">
      <BarChartMetric {group} />
    </div>
  {:else}
    <h3 class="mb-2 flex items-center gap-1 text-base font-semibold text-surface-900-100">
      {group.label}
      {#if help}
        <span class="inline-flex cursor-help text-surface-500"><CircleHelp /></span>
        <Tooltip class="max-w-xs">{help}</Tooltip>
      {/if}
    </h3>
    {#if group.kind === 'K2'}
      <PercentRingMetric {group} />
    {:else if group.kind === 'K3'}
      <OccupancyRingMetric {group} />
    {:else if group.kind === 'K4'}
      <ChipMetric {group} />
    {:else if group.kind === 'K5'}
      <BooleanMetric {group} />
    {:else if group.kind === 'K6'}
      <CompactionStateMetric {group} />
    {:else if group.kind === 'K7'}
      <StatsMetric {group} />
    {:else if group.kind === 'K8'}
      <CacheCountsMetric {group} />
    {:else if group.kind === 'K9'}
      <MergesHistogram {group} />
    {:else if group.kind === 'K10'}
      <KeyDistributionMatrix {group} />
    {:else if group.kind === 'K11'}
      <SizeHistogram {group} />
    {:else}
      <div class="text-sm text-surface-600-400">No renderer for kind {group.kind} yet.</div>
    {/if}
  {/if}
</div>
