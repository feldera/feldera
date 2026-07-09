<script lang="ts" module>
  // Thin wrapper around svelte-echarts. Registers (tree-shaken) the chart types and components
  // the cache tile uses, once for the whole app, then renders any EChartsOption in a sized box.
  import { BarChart, HeatmapChart, LineChart, ScatterChart } from 'echarts/charts'
  import {
    GraphicComponent,
    GridComponent,
    MarkLineComponent,
    TooltipComponent,
    VisualMapComponent
  } from 'echarts/components'
  import { use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'

  use([
    ScatterChart,
    HeatmapChart,
    BarChart,
    LineChart, // marginal density curves in ScatterWithMarginals
    GridComponent,
    TooltipComponent,
    VisualMapComponent,
    MarkLineComponent,
    GraphicComponent,
    CanvasRenderer
  ])
</script>

<script lang="ts">
  import type { EChartsOption } from 'echarts'
  import { init } from 'echarts/core'
  import { Chart } from 'svelte-echarts'

  interface Props {
    options: EChartsOption
    height?: number
    /** Forwarded ECharts click; `e` carries `seriesIndex` / `dataIndex`. */
    onclick?: (e: any) => void
    /** Fired when the click lands on empty canvas (no data element). */
    onblank?: () => void
  }
  const { options, height = 160, onclick, onblank }: Props = $props()

  let instance = $state<any>()

  // Capture the ECharts instance as it is created so we can reach its zrender layer.
  const initChart: typeof init = (dom, theme, opts) => {
    instance = init(dom, theme, opts)
    return instance
  }

  // ECharts' series `click` only fires on elements; a click on empty canvas has no zrender
  // target, which is how we detect "clicked nothing" to clear a selection.
  $effect(() => {
    if (!instance || !onblank) {
      return
    }
    const zr = instance.getZr()
    const handler = (e: any) => {
      if (!e.target) {
        onblank()
      }
    }
    zr.on('click', handler)
    return () => zr.off('click', handler)
  })
</script>

<div class="w-full" style:height="{height}px">
  <Chart init={initChart} {options} {onclick} />
</div>
