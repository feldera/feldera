<script lang="ts">
  import { humanSize } from 'common-lib/string'
  import { tuple } from 'common-lib/tuple'
  import { getThemeColor } from 'common-ui'
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
  import {
    multihostMemoryLimitBytes,
    resourceSizeBytes,
    timeSeriesAxisMax
  } from '#lib/functions/pipelineMetrics.ts'
  import type { MemoryPressure } from '#lib/services/manager/index.ts'
  import type { Pipeline } from '#lib/services/pipelineManager.ts'
  import type { TimeSeriesEntry } from '#lib/types/pipelineManager.ts'

  const {
    pipeline,
    metrics,
    keepMs,
    memoryPressure
  }: {
    pipeline: { current: Pipeline }
    metrics: TimeSeriesEntry[]
    refetchMs: number
    keepMs: number
    memoryPressure?: MemoryPressure
  } = $props()

  const pressureChips: Record<MemoryPressure, { label: string; class: string }> = {
    low: { label: '', class: 'hidden' },
    moderate: { label: 'Moderate pressure', class: 'bg-warning-100-900' },
    high: { label: 'High pressure', class: 'bg-error-50-950' },
    critical: { label: 'Critical pressure', class: 'bg-error-50-950' }
  }
  const pressureChip = $derived(memoryPressure ? pressureChips[memoryPressure] : undefined)
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
  // deployment, while `memory_max` is the per-host limit. Scale the limit by
  // the number of hosts.
  const maxMemoryBytes = $derived(
    multihostMemoryLimitBytes(
      resourceSizeBytes(pipeline.current.runtimeConfig?.resources, 'memory_max', 'memory_mb_max'),
      pipeline.current.runtimeConfig?.hosts
    )
  )

  const primaryColor = getThemeColor('--color-chart-line').format('hex')

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
            data: maxMemoryBytes
              ? [{ yAxis: maxMemoryBytes, lineStyle: { color: 'red', cap: 'square' } }]
              : []
          }
        }
      ]
    })
  })

  const options: EChartsOption = {
    // No animation: each new sample redraws the chart in place instead of sliding it.
    animation: false,
    animationDuration: 0,
    animationDurationUpdate: 0,
    animationEasingUpdate: 'linear' as const,
    // 8px from the card's edges; the axis labels are fitted inside that box rather than given
    // fixed gutters, so they sit 8px from the edge too. The title's own 8px sits above.
    grid: {
      top: 0,
      left: 8,
      right: 8,
      bottom: 8,
      outerBoundsMode: 'same',
      outerBoundsContain: 'axisLabel'
    },
    xAxis: {
      animationDuration: 0,
      animationDurationUpdate: 0,
      type: 'time' as const,
      // Start on the same window the updates below use, so the first update has nothing to
      // animate and the line doesn't slide in when the tab opens.
      // svelte-ignore state_referenced_locally
      min: xAxisMax - keepMs,
      // svelte-ignore state_referenced_locally
      max: xAxisMax,
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
        animationDurationUpdate: 0,
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
          data: maxMemoryBytes
            ? [{ yAxis: maxMemoryBytes, lineStyle: { color: 'red', cap: 'square' } }]
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

<div class="absolute flex h-full w-full flex-col pt-2">
  <div class="flex items-center justify-between gap-2 px-2 pb-2">
    <span class="text-sm">Used memory: {humanSize(metrics.at(-1)?.m ?? 0)}</span>
    {#if pressureChip}
      <div class="pointer-events-none flex h-5 rounded px-2 text-sm {pressureChip.class}">
        {pressureChip.label}
      </div>
    {/if}
  </div>
  <!-- The chart takes only the height left under the title, so its axis labels stay inside the card. -->
  <div class="min-h-0 flex-1">
    {#key pipelineName}
      <Chart init={(dom, theme, opts) => (ref = init(dom, theme, opts))} {options} />
    {/key}
  </div>
</div>
