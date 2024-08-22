<script lang="ts">
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  import { Chart } from 'svelte-echarts'
  import { init, use } from 'echarts/core'
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent } from 'echarts/components'
  import { CanvasRenderer } from 'echarts/renderers'
  import { tuple } from '$lib/functions/common/tuple'
  import { humanSize } from '$lib/functions/common/string'
  import type { EChartsOption } from 'echarts'

  let { metrics }: { metrics: PipelineMetrics } = $props()
  use([LineChart, GridComponent, CanvasRenderer, TitleComponent])

  const memUsed = $derived(metrics.global.map((m) => tuple(m.timeMs, m.rss_bytes ?? 0)))
  const valueMax = $derived(memUsed.length ? Math.max(...memUsed.map((v) => v[1])) : 0)
  const yMaxStep = $derived(Math.pow(2, Math.ceil(Math.log2(valueMax * 1.25))))
  const yMax = $derived(valueMax !== 0 ? yMaxStep : 1024 * 2048)
  const yMin = 0
  $effect(() => {
    console.log('yMin', yMin, yMax)
  })

  let options = $derived({
    animationDurationUpdate: 1000,
    animationEasingUpdate: 'linear',
    grid: {
      top: 10,
      left: 70
    },
    xAxis: {
      type: 'time' as const,
      min: Date.now() - 60 * 1000,
      minInterval: 20000,
      maxInterval: 20000
    },
    yAxis: {
      type: 'value' as const,
      interval: (yMax - yMin) / 2,
      min: yMin,
      max: yMax,
      axisLabel: {
        showMinLabel: true,
        showMaxLabel: true,
        // customValues: [yMin, (yMin + yMax) / 2, yMax],
        formatter: (value: number) => humanSize(value)
      }
    },
    series: [
      {
        type: 'line' as const,
        symbol: 'none',
        // markPoint: {
        //   mainType: 'markPoint'
        // },
        markLine: {
          label: {
            normal: {
              show: false
            }
          }
        },
        data: metrics.global.map((m) => ({
          name: m.timeMs.toString(),
          value: tuple(m.timeMs, m.rss_bytes ?? 0)
        }))
      }
    ]
  } as EChartsOption)
</script>

<span class="pl-20">
  Pipeline Memory: {humanSize(metrics.global.at(-1)?.rss_bytes ?? 0)}
</span>
<div class="absolute h-full w-full">
  <Chart {init} {options} />
</div>
