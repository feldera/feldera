<script lang="ts">
  import { calcPipelineThroughput, type PipelineMetrics } from '$lib/functions/pipelineMetrics'

  import { Chart } from 'svelte-echarts'
  import { init, use } from 'echarts/core'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { CanvasRenderer } from 'echarts/renderers'
  import { tuple } from '$lib/functions/common/tuple'
  import { humanSize } from '$lib/functions/common/string'
  import { format } from 'd3-format'
  import type { EChartsOption } from 'echarts'

  let { metrics }: { metrics: PipelineMetrics } = $props()
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent, TooltipComponent])

  const series = $derived(calcPipelineThroughput(metrics))

  const options = $derived({
    animationDurationUpdate: 1000,
    animationEasingUpdate: 'linear',
    tooltip: {},
    dataLabels: { enabled: false },
    stroke: {
      width: 3,
      curve: 'smooth',
      lineCap: 'round'
    },
    grid: {
      top: 10,
      left: 70
    },
    xAxis: {
      type: 'time',
      min: Date.now() - 60 * 1000,
      minInterval: 20000,
      maxInterval: 20000,
      animationDurationUpdate: 1000,
      animationEasingUpdate: 'linear'
    },
    yAxis: {
      type: 'value' as const,
      interval: (series.yMax - series.yMin) / 2,
      min: series.yMin,
      max: series.yMax,
      axisLabel: {
        formatter(val: number) {
          return format(val >= 1000 ? '.3s' : '.0f')(val)
        }
      }
      // labels: {
      //   formatter(val: number) {
      //     return format(val >= 1000 ? '.3s' : '.0f')(val)
      //   }
      // }
    },
    series: [
      {
        type: 'line' as const,
        symbol: 'none',
        data: series.throughput
      }
    ]
  } satisfies EChartsOption)
</script>

<span class="pl-20">
  Pipeline Throughput: {((v) => format(v >= 1000 ? '.3s' : '.0f')(v))(series.current)} rows/s
</span>
<div class="absolute h-full w-full">
  <Chart {init} {options} />
</div>
