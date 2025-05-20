<script lang="ts">
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'

  import { Chart } from 'svelte-echarts'
  import { init, use, type EChartsType } from 'echarts/core'
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
  import type { Pipeline } from '$lib/services/pipelineManager'
  import type { ECMouseEvent } from 'svelte-echarts'
  import type { EChartsOption } from 'echarts'
  import { rgbToHex } from '$lib/functions/common/color'

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

  let pipelineName = $derived(pipeline.current.name)

  const memUsed = $derived(metrics.global.map((m) => tuple(m.timeMs, m.rss_bytes ?? 0)))
  const valueMax = $derived(memUsed.length ? Math.max(...memUsed.map((v) => v[1])) : 0)
  const yMaxStep = $derived(Math.pow(2, Math.ceil(Math.log2(valueMax * 1.25))))
  const yMax = $derived(valueMax !== 0 ? yMaxStep : 1024 * 2048)
  const yMin = 0
  const maxMemoryMb = $derived(
    pipeline.current.runtimeConfig?.resources?.memory_mb_max ?? undefined
  )

  let primaryColor = rgbToHex(
    getComputedStyle(document.body).getPropertyValue('--color-primary-500').trim()
  )

  let ref: EChartsType | undefined = $state()

  $effect(() => {
    metrics.global
    if (!ref) {
      return
    }
    ref.setOption({
      series: [
        {
          data: metrics.global.map((m) => ({
            name: m.timeMs.toString(),
            value: tuple(m.timeMs, m.rss_bytes ?? 0)
          }))
        }
      ],
      xAxis: {
        min: Date.now() - keepMs,
        max: Date.now()
      },
      yAxis: {
        interval: (yMax - yMin) / 2,
        min: yMin,
        max: yMax
      }
    })
  })

  $effect(() => {
    if (!ref) {
      return
    }
    ref.setOption({
      series: [
        {
          markline: {
            data: maxMemoryMb
              ? [
                  { yAxis: maxMemoryMb * 1000 * 1000, lineStyle: { color: 'red', cap: 'square' } } // example 1
                ]
              : []
          }
        }
      ]
    })
  })

  let options: EChartsOption = {
    animationDuration: 0,
    animationDurationUpdate: refetchMs,
    animationEasingUpdate: 'linear' as const,
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
      },
      animation: true // optional, makes axis transitions smoother
    },
    yAxis: {
      type: 'value' as const,
      // svelte-ignore state_referenced_locally
      interval: (yMax - yMin) / 2,
      min: yMin,
      // svelte-ignore state_referenced_locally
      max: yMax,
      axisLabel: {
        showMinLabel: true,
        showMaxLabel: true,
        formatter: humanSize
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
        return humanSize(x.value[1])
      }
    },
    color: primaryColor,
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
            position: 'end' as const,
            formatter: (v) => humanSize(v.value as number)
          },
          emphasis: { disabled: true },
          symbol: ['none', 'none'],
          // svelte-ignore state_referenced_locally
          data: maxMemoryMb
            ? [
                { yAxis: maxMemoryMb * 1000 * 1000, lineStyle: { color: 'red', cap: 'square' } } // example 1
              ]
            : []
        },
        triggerLineEvent: true
      }
    ]
  }

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

<div class="absolute h-full w-full py-4">
  <div class="pl-16">
    Used memory: {humanSize(metrics.global.at(-1)?.rss_bytes ?? 0)}
  </div>
  {#key pipelineName}
    <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
  {/key}
</div>
