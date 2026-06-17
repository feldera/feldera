<script lang="ts">
  import type { EChartsOption } from 'echarts'
  import { LineChart } from 'echarts/charts'
  import {
    GridComponent,
    MarkLineComponent,
    TitleComponent,
    TooltipComponent
  } from 'echarts/components'
  import { type EChartsType, init, use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import type { ECMouseEvent } from 'svelte-echarts'
  import { Chart } from 'svelte-echarts'
  import { ServerDate } from '$lib/compositions/serverTime'
  import { getThemeColor } from '$lib/functions/common/color'
  import { humanSize } from '$lib/functions/common/string'
  import { tuple } from '$lib/functions/common/tuple'
  import { multihostMemoryLimitMb, timeSeriesAxisMax } from '$lib/functions/pipelineMetrics'
  import type { Pipeline } from '$lib/services/pipelineManager'
  import type { TimeSeriesEntry } from '$lib/types/pipelineManager'

  const {
    pipeline,
    metrics,
    refetchMs,
    keepMs
  }: {
    pipeline: { current: Pipeline }
    metrics: TimeSeriesEntry[]
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

  const pipelineName = $derived(pipeline.current.name)

  // Anchor the time axis to the newest sample's timestamp rather than to the
  // client clock.
  const xAxisMax = $derived(timeSeriesAxisMax(metrics))

  const valueMax = $derived(metrics.length ? Math.max(...metrics.map((v) => v.m)) : 0)
  const yMaxStep = $derived(2 ** Math.ceil(Math.log2(valueMax * 1.25)))
  const yMax = $derived(valueMax !== 0 ? yMaxStep : 1024 * 2048)
  const yMin = 0
  // The reported memory metric (`m`) is the sum across all hosts in a multihost
  // deployment, while `memory_mb_max` is the per-host limit. Scale the limit by
  // the number of hosts.
  const maxMemoryMb = $derived(
    multihostMemoryLimitMb(
      pipeline.current.runtimeConfig?.resources?.memory_mb_max,
      pipeline.current.runtimeConfig?.hosts
    )
  )

  const primaryColor = getThemeColor('--color-primary-500').format('hex')

  let ref: EChartsType | undefined = $state()

  $effect(() => {
    metrics
    if (!ref) {
      return
    }
    ref.setOption({
      series: [
        {
          data: metrics.map((m) => ({
            id: m.t,
            value: tuple(m.t, m.m ?? 0)
          }))
        }
      ],
      xAxis: {
        min: xAxisMax - keepMs,
        max: xAxisMax
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
              ? [{ yAxis: maxMemoryMb * 1000 * 1000, lineStyle: { color: 'red', cap: 'square' } }]
              : []
          }
        }
      ]
    })
  })

  const options: EChartsOption = {
    animationDuration: 0,
    animationDurationUpdate: 0,
    animationEasingUpdate: 'linear' as const,
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
      min: ServerDate.now() - keepMs - refetchMs,
      // svelte-ignore state_referenced_locally
      max: ServerDate.now() - refetchMs,
      minInterval: 25000,
      maxInterval: 25000,
      axisLabel: {
        formatter: (ms: number) => new Date(ms).toLocaleTimeString()
      },
      animation: true // optional, makes axis transitions smoother
    },
    yAxis: {
      animationDuration: 0,
      animationDurationUpdate: 0,
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
        animationDuration: 0,
        animationDurationUpdate: refetchMs,
        type: 'line' as const,
        // symbol: 'none',
        itemStyle: {
          opacity: 0
        },
        data: metrics.map((m) => ({
          id: m.t,
          value: tuple(m.t, m.m ?? 0)
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
            ? [{ yAxis: maxMemoryMb * 1000 * 1000, lineStyle: { color: 'red', cap: 'square' } }]
            : []
        },
        triggerLineEvent: true
      }
    ]
  }

  const handleSeriesHover = <T>(setValue: (value: T | null) => void) => ({
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
  <div class="px-4 pb-2">
    Used memory: {humanSize(metrics.at(-1)?.m ?? 0)}
  </div>
  {#key pipelineName}
    <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
  {/key}
</div>
