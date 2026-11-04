<script lang="ts">
  import { formatQty } from 'common-lib/format'
  import { getThemeColor } from 'common-ui'
  import type { EChartsOption } from 'echarts'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { type EChartsType, init, use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { Chart } from 'svelte-echarts'
  import {
    calcPipelineThroughput,
    type PipelineMetrics,
    timeSeriesAxisMax
  } from '#lib/functions/pipelineMetrics.ts'
  import type { Pipeline } from '#lib/services/pipelineManager.ts'
  import type { TimeSeriesEntry } from '#lib/types/pipelineManager.ts'

  const {
    pipeline,
    metrics,
    keepMs
  }: {
    metrics: TimeSeriesEntry[]
    refetchMs: number
    keepMs: number
    pipeline: { current: Pipeline }
  } = $props()
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent, TooltipComponent])

  const pipelineName = $derived(pipeline.current.name)
  const throughput = $derived(calcPipelineThroughput(metrics))

  // Anchor the time axis to the newest sample's timestamp rather than to the
  // client clock.
  const xAxisMax = $derived(timeSeriesAxisMax(metrics))

  const primaryColor = getThemeColor('--color-chart-line').format('hex')

  let ref: EChartsType | undefined = $state()

  $effect(() => {
    throughput.series
    if (!ref) {
      return
    }
    ref.setOption({
      series: [
        {
          data: throughput.series.map((p) => ({
            id: p.value[0],
            value: p.value
          }))
        }
      ],
      xAxis: {
        min: xAxisMax - keepMs,
        max: xAxisMax
      },
      yAxis: {
        interval: (throughput.yMax - throughput.yMin) / 2,
        min: throughput.yMin,
        max: throughput.yMax
      }
    })
  })

  const options: EChartsOption = {
    // No animation: each new sample redraws the chart in place instead of sliding it.
    animation: false,
    animationDuration: 0,
    animationDurationUpdate: 0,
    animationEasingUpdate: 'linear' as const,
    dataLabels: { enabled: false },
    // 8px from the card's edges; the axis labels are fitted inside that box rather than given
    // fixed gutters, so they sit 8px from the edge too. The title's own 8px sits above.
    grid: {
      top: 0,
      left: 8,
      right: 8,
      bottom: 8,
      outerBoundsMode: 'same',
      outerBoundsContain: 'axisLabel'
    },
    xAxis: {
      animationDuration: 0,
      animationDurationUpdate: 0,
      type: 'time' as const,
      // Start on the same window the updates below use, so the first update has nothing to
      // animate and the line doesn't slide in when the tab opens.
      // svelte-ignore state_referenced_locally
      min: xAxisMax - keepMs,
      // svelte-ignore state_referenced_locally
      max: xAxisMax,
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
      interval: (throughput.yMax - throughput.yMin) / 2,
      // svelte-ignore state_referenced_locally
      min: throughput.yMin,
      // svelte-ignore state_referenced_locally
      max: throughput.yMax,
      axisLabel: {
        formatter(val: number) {
          return formatQty(val, 'rounded')
        }
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
      position: 'top' as const,
      formatter: (x: any) => {
        return formatQty(x.value[1], 'rounded')
      }
    },
    color: primaryColor,
    series: [
      {
        animationDuration: 0,
        animationDurationUpdate: 0,
        type: 'line' as const,
        itemStyle: {
          opacity: 0
        },
        // svelte-ignore state_referenced_locally
        data: throughput.series.map((p) => ({
          id: p.value[0],
          value: p.value
        }))
      }
    ]
  }
</script>

<div class="absolute flex h-full w-full flex-col pt-2">
  <div class="px-2 pb-2 text-[12px] leading-[18px] whitespace-nowrap">
    Throughput: {formatQty(throughput.current, 'rounded')} records/s
  </div>
  <!-- The chart takes only the height left under the title, so its axis labels stay inside the card. -->
  <div class="min-h-0 flex-1">
    {#key pipelineName}
      <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
    {/key}
  </div>
</div>
