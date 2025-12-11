<script lang="ts">
  import { format } from 'd3-format'
  import type { EChartsOption } from 'echarts'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { type EChartsType, init, use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { Chart } from 'svelte-echarts'
  import { getThemeColor } from '$lib/functions/common/color'
  import { calcPipelineThroughput, type PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { Pipeline } from '$lib/services/pipelineManager'
  import type { TimeSeriesEntry } from '$lib/types/pipelineManager'

  const formatQty = (v: number) => format(v >= 1000 ? '.3s' : '.0f')(v)

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
          data: throughput.series
        }
      ],
      xAxis: {
        min: Date.now() - keepMs,
        max: Date.now()
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
      min: Date.now() - keepMs - refetchMs,
      max: Date.now() - refetchMs,
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
          return formatQty(val)
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
        return formatQty(x.value[1])
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
        data: throughput.series
      }
    ]
  }
</script>

<div class="absolute h-full w-full py-4">
  <div class="pl-16 whitespace-nowrap">
    Throughput: {formatQty(throughput.current)} records/s
  </div>
  {#key pipelineName}
    <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
  {/key}
</div>
