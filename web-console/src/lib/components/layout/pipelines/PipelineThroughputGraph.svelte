<script lang="ts">
  import { calcPipelineThroughput, type PipelineMetrics } from '$lib/functions/pipelineMetrics'

  import { Chart } from 'svelte-echarts'
  import { init, use } from 'echarts/core'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { CanvasRenderer } from 'echarts/renderers'
  import { format } from 'd3-format'
  import type { EChartsOption } from 'echarts'

  const formatQty = (v: number) => format(v >= 1000 ? '.3s' : '.0f')(v)

  let {
    metrics,
    refetchMs,
    keepMs
  }: { metrics: PipelineMetrics; refetchMs: number; keepMs: number } = $props()
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent, TooltipComponent])

  const throughput = $derived(calcPipelineThroughput(metrics))

  const options = $derived({
    animationDuration: 0,
    animationDurationUpdate: refetchMs * 1.5,
    animationEasingUpdate: 'linear',
    dataLabels: { enabled: false },
    grid: {
      top: 10,
      left: 64,
      right: 20,
      bottom: 48
    },
    xAxis: {
      type: 'time',
      min: Date.now() - keepMs,
      minInterval: 25000,
      maxInterval: 25000,
      axisLabel: {
        formatter: (ms: number) => new Date(ms).toLocaleTimeString()
      }
    },
    yAxis: {
      type: 'value' as const,
      interval: (throughput.yMax - throughput.yMin) / 2,
      min: throughput.yMin,
      max: throughput.yMax,
      axisLabel: {
        formatter(val: number) {
          return formatQty(val)
        }
      }
    },
    tooltip: {
      show: true,
      position: 'top',
      formatter: (x: any) => {
        return formatQty(x.value[1])
      }
    },
    series: [
      {
        type: 'line' as const,
        // symbol: 'none',
        itemStyle: {
          opacity: 0
        },
        data: throughput.series
      }
    ]
  } satisfies EChartsOption)
</script>

<span class="absolute whitespace-nowrap pl-16">
  Throughput: {formatQty(throughput.current)} records/s
</span>
<div class="absolute mt-6 h-full w-full">
  <Chart {init} {options} />
</div>
