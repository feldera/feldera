<script lang="ts">
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  import * as echarts from 'echarts'
  import { Chart } from 'svelte-echarts'
  import { init, use } from 'echarts/core'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent } from 'echarts/components'
  import { CanvasRenderer } from 'echarts/renderers'
  import { tuple } from '$lib/functions/common/tuple'
  import { humanSize } from '$lib/functions/common/string'

  let { metrics }: { metrics: PipelineMetrics } = $props()
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent])

  const memUsed = metrics.global.map((m) => tuple(m.timeMs, m.rss_bytes ?? 0))
  const valueMax = $derived(memUsed.length ? Math.max(...memUsed.map((v) => v[1])) : 0)
  const yMaxStep = $derived(Math.pow(2, Math.ceil(Math.log2(valueMax * 1.25))))
  $effect(() => {
    console.log('valueMax', valueMax, yMaxStep)
  })
  const yMax = $derived(valueMax !== 0 ? yMaxStep : 1024 * 2048)
  const yMin = 0

  let options = $derived({
    title: {
      text: 'ECharts Example'
    },
    xAxis: {
      type: 'time' as const
    },
    yAxis: {
      type: 'value' as const,
      splitNumber: 3,
      min: yMin,
      max: yMax,
      axisLabel: {
        formatter: (value: number) => humanSize(value)
      }
    },
    series: [
      {
        type: 'line' as const,
        data: metrics.global.map((m) => tuple(m.timeMs, m.rss_bytes ?? 0))
      }
    ]
  })
</script>

<Chart {init} {options} />
