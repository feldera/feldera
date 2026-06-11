<script lang="ts">
  import type { EChartsOption } from 'echarts'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { type EChartsType, init, use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { Chart } from 'svelte-echarts'
  import { dateNow } from '$lib/compositions/serverTime'
  import { getThemeColor } from '$lib/functions/common/color'
  import { formatQty } from '$lib/functions/format'
  import { calcPipelineThroughput, type PipelineMetrics } from '$lib/functions/pipelineMetrics'
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
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent, TooltipComponent])

  const pipelineName = $derived(pipeline.current.name)
  const throughput = $derived(calcPipelineThroughput(metrics))

  // Anchor the time axis to the newest sample's timestamp rather than to the
  // client clock.
  const xAxisMax = $derived(metrics.at(-1)?.t ?? dateNow())

  const primaryColor = getThemeColor('--color-primary-500').format('hex')

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
    animationDuration: 0,
    animationDurationUpdate: 0,
    animationEasingUpdate: 'linear' as const,
    dataLabels: { enabled: false },
    grid: {
      top: 10,
      left: 64,
      right: 50,
      bottom: 48
    },
    xAxis: {
      animationDuration: 0,
      animationDurationUpdate: refetchMs,
      type: 'time' as const,
      // svelte-ignore state_referenced_locally
      min: dateNow() - keepMs - refetchMs,
      // svelte-ignore state_referenced_locally
      max: dateNow() - refetchMs,
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
        animationDurationUpdate: refetchMs,
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

<div class="absolute h-full w-full py-4">
  <div class="px-4 pb-2 whitespace-nowrap">
    Throughput: {formatQty(throughput.current, 'rounded')} records/s
  </div>
  {#key pipelineName}
    <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
  {/key}
</div>
