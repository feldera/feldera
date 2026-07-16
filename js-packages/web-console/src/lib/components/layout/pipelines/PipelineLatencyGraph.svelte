<script lang="ts">
  import type { EChartsOption } from 'echarts'
  import { LineChart } from 'echarts/charts'
  import {
    GridComponent,
    LegendComponent,
    TitleComponent,
    TooltipComponent
  } from 'echarts/components'
  import { type EChartsType, init, use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { Chart } from 'svelte-echarts'
  import { ServerDate } from '$lib/compositions/serverTime'
  import { getThemeColor } from '$lib/functions/common/color'
  import { formatDuration } from '$lib/functions/format'
  import { calcPipelineLatency, timeSeriesAxisMax } from '$lib/functions/pipelineMetrics'
  import type { Pipeline } from '$lib/services/pipelineManager'
  import type { TimeSeriesEntry } from '$lib/types/pipelineManager'

  const {
    pipeline,
    metrics,
    refetchMs,
    keepMs
  }: {
    metrics: TimeSeriesEntry[]
    refetchMs: number
    keepMs: number
    pipeline: { current: Pipeline }
  } = $props()
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent, TooltipComponent, LegendComponent])

  const pipelineName = $derived(pipeline.current.name)
  const latency = $derived(calcPipelineLatency(metrics))
  const xAxisMax = $derived(timeSeriesAxisMax(metrics))

  // Latest p50 (baseline) of each family, for the title. Completion is shown as
  // its extra cost over processing (the downstream/delivery portion), since
  // completion latency includes processing.
  const currentProcessing = $derived(latency.series.processingP50.at(-1)?.[1])
  const currentCompletion = $derived(latency.series.completionP50.at(-1)?.[1])
  const completionDelta = $derived(
    currentProcessing !== undefined && currentCompletion !== undefined
      ? Math.max(0, currentCompletion - currentProcessing)
      : undefined
  )

  // Hue distinguishes the latency family, line style the percentile, so the
  // chart stays legible for colour-blind viewers and in grayscale.
  const processingColor = getThemeColor('--color-primary-600').format('hex')
  const completionColor = '#E69F00' // Okabe-Ito orange

  // The p50 series carry the family names, so the 2-item legend maps to them and
  // shows their (solid) line colour.
  const processingName = 'step processing'
  const completionName = 'end-to-end completion'
  const seriesDefs = [
    {
      key: 'processingP50' as const,
      name: `${processingName} median`,
      color: processingColor,
      style: 'solid',
      isP99: false
    },
    {
      key: 'processingP99' as const,
      name: `${processingName} (p99)`,
      color: processingColor,
      style: 'dotted',
      isP99: true
    },
    {
      key: 'completionP50' as const,
      name: `${completionName} median`,
      color: completionColor,
      style: 'solid',
      isP99: false
    },
    {
      key: 'completionP99' as const,
      name: `${completionName} (p99)`,
      color: completionColor,
      style: 'dotted',
      isP99: true
    }
  ]

  let showP99 = $state(false)
  let ref: EChartsType | undefined = $state()

  // Drop the "processing"/"completion" words when the full title would not fit
  // beside the toggle. Measured against a hidden full-width span so hiding the
  // words can't shrink the measurement and cause it to flip back (oscillate).
  let availWidth = $state(0)
  let fullWidth = $state(0)
  const compactTitle = $derived(fullWidth > 0 && availWidth > 0 && fullWidth > availWidth)

  const toData = (points: [number, number][]) => points.map((p) => ({ id: p[0], value: p }))

  // A p99 series is emptied rather than removed when hidden, so its colour/style
  // slot stays stable across toggles.
  const seriesData = (d: (typeof seriesDefs)[number], show99: boolean) =>
    d.isP99 && !show99 ? [] : toData(latency.series[d.key])

  const yMaxFor = (show99: boolean) => (show99 ? latency.yMax : latency.yMaxBaseline)

  $effect(() => {
    latency.series
    if (!ref) {
      return
    }
    const yMax = yMaxFor(showP99)
    ref.setOption({
      series: seriesDefs.map((d) => ({ data: seriesData(d, showP99) })),
      xAxis: {
        min: xAxisMax - keepMs,
        max: xAxisMax
      },
      yAxis: {
        interval: (yMax - latency.yMin) / 2,
        min: latency.yMin,
        max: yMax
      }
    })
  })

  const options: EChartsOption = {
    animationDuration: 0,
    animationDurationUpdate: 0,
    animationEasingUpdate: 'linear' as const,
    grid: {
      // Match the sibling tiles' bottom/left/right so the x-axis lines up; the
      // extra top room holds the legend.
      top: 28,
      left: 64,
      right: 50,
      bottom: 48
    },
    legend: {
      show: true,
      top: 0,
      left: 'center' as const,
      selectedMode: false,
      data: seriesDefs.filter((d) => !d.isP99).map((d) => d.name),
      formatter: (name: string) => name.replace(/ median$/, ''),
      itemWidth: 20,
      itemHeight: 8,
      textStyle: { fontSize: 12 }
    },
    xAxis: {
      animationDuration: 0,
      // svelte-ignore state_referenced_locally
      animationDurationUpdate: refetchMs,
      type: 'time' as const,
      // svelte-ignore state_referenced_locally
      min: ServerDate.now() - keepMs - refetchMs,
      // svelte-ignore state_referenced_locally
      max: ServerDate.now() - refetchMs,
      minInterval: 25000,
      maxInterval: 25000,
      axisLabel: {
        formatter: (ms: number) => new Date(ms).toLocaleTimeString()
      }
    },
    yAxis: {
      animationDuration: 0,
      animationDurationUpdate: 0,
      type: 'value' as const,
      // svelte-ignore state_referenced_locally
      interval: (yMaxFor(showP99) - latency.yMin) / 2,
      // svelte-ignore state_referenced_locally
      min: latency.yMin,
      // svelte-ignore state_referenced_locally
      max: yMaxFor(showP99),
      axisLabel: {
        formatter: (val: number) => formatDuration(val)
      },
      splitLine: {
        lineStyle: {
          color: 'gray' as const,
          opacity: 0.5
        }
      }
    },
    tooltip: {
      show: true,
      trigger: 'axis' as const,
      formatter: (params: any) =>
        (Array.isArray(params) ? params : [params])
          .map((p) => `${p.marker}${p.seriesName}: ${formatDuration(p.value[1])}`)
          .join('<br/>')
    },
    // svelte-ignore state_referenced_locally
    series: seriesDefs.map((d) => ({
      name: d.name,
      animationDuration: 0,
      animationDurationUpdate: refetchMs,
      type: 'line' as const,
      showSymbol: false,
      itemStyle: { color: d.color },
      lineStyle: { color: d.color, type: d.style as 'solid' | 'dotted' },
      // svelte-ignore state_referenced_locally
      data: seriesData(d, showP99)
    }))
  }
</script>

<div class="absolute h-full w-full py-4">
  <!-- One-line flow header, same height as the sibling tiles, so the chart
       canvas (and its x-axis) sits at the same vertical position. -->
  <div class="flex items-center gap-2 px-4 pb-2">
    <div bind:clientWidth={availWidth} class="min-w-0 flex-1 overflow-hidden whitespace-nowrap">
      {#if compactTitle}
        Latency: {formatDuration(currentProcessing)} + {formatDuration(completionDelta)}
      {:else}
        Latency: {formatDuration(currentProcessing)} processing + {formatDuration(completionDelta)} completion
      {/if}
    </div>
    <label class="flex shrink-0 cursor-pointer items-center gap-1 text-sm">
      p99
      <input type="checkbox" class="checkbox" bind:checked={showP99} />
    </label>
  </div>
  <!-- Hidden measurer: the full title's natural width, for the compact decision. -->
  <span
    aria-hidden="true"
    bind:clientWidth={fullWidth}
    class="pointer-events-none invisible absolute top-0 whitespace-nowrap"
  >
    Latency: {formatDuration(currentProcessing)} processing + {formatDuration(completionDelta)} completion
  </span>
  {#key pipelineName}
    <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
  {/key}
</div>
