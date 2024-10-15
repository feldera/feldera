<script lang="ts">
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  import { Chart } from 'svelte-echarts'
  import { init, use } from 'echarts/core'
  import { LineChart } from 'echarts/charts'
  import {
    GridComponent,
    MarkLineComponent,
    TitleComponent,
    TooltipComponent
  } from 'echarts/components'
  import { CanvasRenderer } from 'echarts/renderers'
  import { tuple } from '$lib/functions/common/tuple'
  import { humanSize } from '$lib/functions/common/string'
  import type { EChartsInitOpts } from 'echarts/core'
  import type { Pipeline } from '$lib/services/pipelineManager'
  import type { ECMouseEvent } from 'svelte-echarts'
  import { format } from 'd3-format'

  let {
    pipeline,
    metrics,
    refetchMs,
    keepMs
  }: {
    pipeline: { current: Pipeline }
    metrics: PipelineMetrics
    refetchMs: number
    keepMs: number
  } = $props()
  use([
    LineChart,
    GridComponent,
    CanvasRenderer,
    TitleComponent,
    MarkLineComponent,
    TooltipComponent
  ])

  const memUsed = $derived(metrics.global.map((m) => tuple(m.timeMs, m.rss_bytes ?? 0)))
  const valueMax = $derived(memUsed.length ? Math.max(...memUsed.map((v) => v[1])) : 0)
  const yMaxStep = $derived(Math.pow(2, Math.ceil(Math.log2(valueMax * 1.25))))
  const yMax = $derived(valueMax !== 0 ? yMaxStep : 1024 * 2048)
  const yMin = 0
  const maxMemoryMb = $derived(pipeline.current.runtimeConfig.resources?.memory_mb_max ?? undefined)

  let options = $derived({
    animationDuration: 0,
    animationDurationUpdate: refetchMs * 1.5,
    animationEasingUpdate: 'linear',
    grid: {
      top: 10,
      left: 64,
      right: 50,
      bottom: 48
    },
    xAxis: {
      type: 'time' as const,
      min: Math.round(metrics.global.at(-1)?.timeMs ?? Date.now()) - keepMs,
      minInterval: 25000,
      maxInterval: 25000,
      axisLabel: {
        formatter: (ms: number) => new Date(ms).toLocaleTimeString()
      }
    },
    yAxis: {
      type: 'value' as const,
      interval: (yMax - yMin) / 2,
      min: yMin,
      max: yMax,
      axisLabel: {
        showMinLabel: true,
        showMaxLabel: true,
        formatter: humanSize
      },
      splitLine: {
        lineStyle: {
          color: 'gray',
          opacity: 0.5
        }
      }
    },
    tooltip: {
      show: true,
      position: 'top',
      formatter: (x: any) => {
        return humanSize(x.value[1])
      }
    },
    series: [
      {
        type: 'line' as const,
        // symbol: 'none',
        itemStyle: {
          opacity: 0
        },
        data: metrics.global.map((m) => ({
          name: m.timeMs.toString(),
          value: tuple(m.timeMs, m.rss_bytes ?? 0)
        })),
        markLine: {
          animation: false,
          tooltip: {
            show: false
          },
          label: {
            position: 'end',
            formatter: (v) => humanSize(v.value as number)
          },
          emphasis: { disabled: true },
          symbol: ['none', 'none'],
          data: maxMemoryMb
            ? [
                { yAxis: maxMemoryMb * 1000 * 1000, lineStyle: { color: 'red', cap: 'square' } } // example 1
              ]
            : []
        },
        triggerLineEvent: true
      }
    ]
  } as EChartsInitOpts)

  const handleSeriesHover = <T,>(setValue: (value: T | null) => void) => ({
    mouseover: (e: CustomEvent<ECMouseEvent>) => {
      if (e.detail.componentType !== 'series') {
        return
      }
      if (!Array.isArray(e.detail.value)) {
        return
      }
      setValue((e.detail.value as any)[1])
    },
    mouseout: () => setValue(null)
  })
</script>

<span class="pl-16">
  Used memory: {humanSize(metrics.global.at(-1)?.rss_bytes ?? 0)}
</span>
<div class="absolute h-full w-full">
  <Chart {init} {options} />
</div>
