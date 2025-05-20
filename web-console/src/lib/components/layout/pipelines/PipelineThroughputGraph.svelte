<script lang="ts">
  import { calcPipelineThroughput, type PipelineMetrics } from '$lib/functions/pipelineMetrics'

  import { Chart } from 'svelte-echarts'
  import { init, use, type EChartsType } from 'echarts/core'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { CanvasRenderer } from 'echarts/renderers'
  import { format } from 'd3-format'
  import type { Pipeline } from '$lib/services/pipelineManager'
  import type { EChartsOption } from 'echarts'
  import { rgbToHex } from '$lib/functions/common/color'

  const formatQty = (v: number) => format(v >= 1000 ? '.3s' : '.0f')(v)

  let {
    pipeline,
    metrics,
    refetchMs,
    keepMs
  }: {
    metrics: PipelineMetrics
    refetchMs: number
    keepMs: number
    pipeline: { current: Pipeline }
  } = $props()
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent, TooltipComponent])

  let pipelineName = $derived(pipeline.current.name)
  const throughput = $derived(calcPipelineThroughput(metrics))

  let primaryColor = rgbToHex(
    getComputedStyle(document.body).getPropertyValue('--color-primary-500').trim()
  )

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
    animationDurationUpdate: refetchMs,
    animationEasingUpdate: 'linear' as const,
    dataLabels: { enabled: false },
    grid: {
      top: 10,
      left: 64,
      right: 50,
      bottom: 48
    },
    xAxis: {
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
  <div class="whitespace-nowrap pl-16">
    Throughput: {formatQty(throughput.current)} records/s
  </div>
  {#key pipelineName}
    <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
  {/key}
</div>
