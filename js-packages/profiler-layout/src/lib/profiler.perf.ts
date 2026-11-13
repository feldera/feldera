// The profiler performance suite. `bun run test-perf` runs it with `vitest.perf.config.ts`.
// Each measurement times the real code path, from the user action until the browser paints the
// result. The tests check only that each action completed. They do not check a time budget.

import { clearLayoutCache, type ProfilerCallbacks } from 'profiler-lib'
import { afterAll, describe, expect, inject, it } from 'vitest'
import { commands } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import ProfilerDiagram from './components/ProfilerDiagram.svelte'
import {
  buildCircuitProfile,
  getSuitableProfiles,
  processProfileFiles
} from './functions/processZipBundle'
import {
  afterNextPaint,
  measure,
  measureFrames,
  type PerfResult,
  summarize,
  timeAction
} from './test-support/perf'
import { syntheticBundle } from './test-support/syntheticBundle'
import { type SyntheticCircuit, syntheticCircuit } from './test-support/syntheticCircuit'

const WORKERS = 8
/** The circuit of the diagram measurements. The ELK layout time grows fast with the number of nodes
 *  on screen, so this circuit is smaller than a large real circuit. The smaller size keeps the suite
 *  at about half a minute. */
const CIRCUIT = syntheticCircuit({
  seed: 1,
  topLevelItems: inject('perfTopLevelItems'),
  workers: WORKERS
})
/** The circuit of the parsing measurements, which do not lay out the circuit. This circuit has about
 *  2400 operators, the size of a large real circuit. */
const LARGE_CIRCUIT = syntheticCircuit({ seed: 1, topLevelItems: 200, workers: WORKERS })
const DIAGRAM_SIZE = { width: 1200, height: 800 }
/** The time after which an action whose layout did not end fails, so the test does not wait for
 *  the test timeout. */
const LAYOUT_TIMEOUT_MS = 60_000
const REGION_WARMUPS = 1
const REGION_RUNS = 9
/** How many top-level regions the frame measurement expands. With the default circuit size, the
 *  expanded diagram has about five times as many nodes as the collapsed diagram (314 and 61). */
const EXPANDED_REGIONS = 10

const describeCircuit = (circuit: SyntheticCircuit) => ({
  topLevelItems: circuit.profile.graph.nodes.nodes.length,
  operators: circuit.operatorCount,
  regions: circuit.regionCount,
  edges: circuit.profile.graph.edges.length,
  workers: WORKERS
})

const results: PerfResult[] = []
afterAll(async () => {
  await commands.savePerfResults({
    circuits: { diagram: describeCircuit(CIRCUIT), parsing: describeCircuit(LARGE_CIRCUIT) },
    results
  })
})

/** Mounts the diagram of `circuit`. Returns helpers that wait until the diagram finishes the work
 *  that a user action started. */
function mountProfiler(circuit: SyntheticCircuit) {
  const wrapper = document.createElement('div')
  wrapper.style.cssText = `position:relative;width:${DIAGRAM_SIZE.width}px;height:${DIAGRAM_SIZE.height}px`
  document.body.appendChild(wrapper)

  let onLayoutDone: (() => void) | undefined
  const reported = {
    hoveredNodes: [] as Array<string | null>,
    /** The worker columns of the last node report. */
    workerColumns: [] as string[],
    errors: [] as string[]
  }
  const callbacks: ProfilerCallbacks = {
    displayNodeAttributes: (data) => {
      reported.hoveredNodes.push(data.match({ some: (v) => v.nodeId, none: () => null }))
      reported.workerColumns = data.match({ some: (v) => v.columns, none: () => [] })
    },
    displayTopNodes: () => {},
    onMetricsChanged: () => {},
    displayMessage: () => {},
    onError: (error) => {
      reported.errors.push(error)
    },
    onRenderingChange: (inFlight) => {
      if (!inFlight) {
        onLayoutDone?.()
        onLayoutDone = undefined
      }
    }
  }

  /** Resolves when the next layout ends and the browser paints its result. The caller must call
   *  this function before the action that starts the layout. */
  const layoutPainted = async () => {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`No layout ended within ${LAYOUT_TIMEOUT_MS} ms`)),
        LAYOUT_TIMEOUT_MS
      )
      onLayoutDone = () => {
        clearTimeout(timer)
        resolve()
      }
    })
    await afterNextPaint()
  }

  const loaded = layoutPainted()
  const screen = render(ProfilerDiagram, {
    target: wrapper,
    props: {
      profile: buildCircuitProfile(circuit.profile, circuit.dataflow, circuit.programCode),
      callbacks
    }
  })
  const container = wrapper.querySelector('.visualizer-graph') as HTMLElement & {
    // biome-ignore lint/suspicious/noExplicitAny: cytoscape stores an untyped registry on its container
    _cyreg?: { cy: any }
  }

  /** Moves the mouse to a rendered position on the canvas, so that cytoscape finds the node at that
   *  position itself, as for a real pointer. */
  const moveMouse = (x: number, y: number) => {
    const rect = container.getBoundingClientRect()
    const surface = container.querySelector('canvas') ?? container
    surface.dispatchEvent(
      new MouseEvent('mousemove', { clientX: rect.left + x, clientY: rect.top + y, bubbles: true })
    )
  }

  return {
    loaded,
    layoutPainted,
    reported,
    moveMouse,
    // biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance, read after `loaded`
    cy: () => container._cyreg!.cy as any,
    diagram: screen.component as unknown as {
      selectMetric(metricId: string): void
      toggleWorker(workerId: string): void
      search(query: string): void
    },
    unmount: () => {
      screen.unmount()
      wrapper.remove()
    }
  }
}

/** Times the hover highlight of the source operator at the current zoom. The source operator is
 *  upstream of most of the circuit, so its hover trace colors the most edges. */
async function measureHover(mounted: ReturnType<typeof mountProfiler>, name: string) {
  mounted.diagram.search(CIRCUIT.sourceOperatorId)
  await afterNextPaint()
  const node = mounted.cy().$id(CIRCUIT.sourceOperatorId)
  const { x, y } = node.renderedPosition()
  return measure(name, {
    warmups: 5,
    runs: 25,
    run: async () => {
      mounted.moveMouse(x, y)
      await afterNextPaint()
    },
    teardown: async () => {
      // The search above already marked the node, so only the node report shows the hover.
      expect(mounted.reported.hoveredNodes.at(-1)).toBe(CIRCUIT.sourceOperatorId)
      // The bottom right corner is empty, and away from the minimap at the top left.
      mounted.moveMouse(DIAGRAM_SIZE.width - 2, DIAGRAM_SIZE.height - 2)
      await afterNextPaint()
    }
  })
}

/** Times switches between two metrics, until the browser paints the new shading. */
async function measureMetricSwitch(mounted: ReturnType<typeof mountProfiler>, name: string) {
  const metrics = ['runtime_percent', 'used_memory_bytes']
  const shading = () =>
    mounted
      .cy()
      .nodes()
      .map((n: { data(key: string): unknown }) => n.data('value'))
  let switches = 0
  const result = await measure(name, {
    warmups: 3,
    runs: 15,
    run: async () => {
      switches++
      mounted.diagram.selectMetric(metrics[switches % 2]!)
      await afterNextPaint()
    }
  })
  // The node shading depends on the metric, so a switch that took effect changes the shading.
  const before = JSON.stringify(shading())
  mounted.diagram.selectMetric(metrics[(switches + 1) % 2]!)
  expect(JSON.stringify(shading())).not.toBe(before)
  return result
}

/** Times the toggle of the first worker, until the browser paints the new shading and the pinned
 *  node report. The runs hide and show the worker in turn. */
async function measureWorkerToggle(mounted: ReturnType<typeof mountProfiler>, name: string) {
  mounted.cy().$id(CIRCUIT.sourceOperatorId).emit('click')
  await afterNextPaint()
  const columnsBefore = mounted.reported.workerColumns.length
  expect(columnsBefore).toBe(WORKERS)
  let toggles = 0
  const result = await measure(name, {
    warmups: 2,
    runs: 16,
    run: async () => {
      toggles++
      mounted.diagram.toggleWorker('0')
      await afterNextPaint()
    }
  })
  // An even number of toggles shows the worker again.
  expect(toggles % 2).toBe(0)
  expect(mounted.reported.workerColumns.length).toBe(columnsBefore)
  mounted.diagram.toggleWorker('0')
  expect(mounted.reported.workerColumns.length).toBe(columnsBefore - 1)
  mounted.diagram.toggleWorker('0')
  return result
}

describe('profiler performance', () => {
  it('measures the parsing of a support bundle', async () => {
    const bundle = await syntheticBundle(LARGE_CIRCUIT)
    let workers = 0
    results.push(
      await measure('parse bundle (unzip + processProfileFiles)', {
        warmups: 3,
        runs: 15,
        run: async () => {
          const collections = getSuitableProfiles(bundle)
          const processed = await processProfileFiles(collections.at(-1)![1])
          workers = processed.profile!.worker_count
        }
      })
    )
    expect(workers).toBe(WORKERS)
  })

  it('measures building the profile model', async () => {
    let operators = 0
    results.push(
      await measure('build model (CircuitProfile.fromJson + setDataflow)', {
        warmups: 3,
        runs: 25,
        run: () => {
          const profile = buildCircuitProfile(
            LARGE_CIRCUIT.profile,
            LARGE_CIRCUIT.dataflow,
            LARGE_CIRCUIT.programCode
          )
          operators = profile.simpleNodes.size
        }
      })
    )
    expect(operators).toBe(LARGE_CIRCUIT.operatorCount)
  })

  it('measures the first load of the diagram', async () => {
    let mounted: ReturnType<typeof mountProfiler> | undefined
    let nodeCount = 0
    const load = (name: string, isCached: boolean) =>
      measure(name, {
        warmups: 1,
        runs: 5,
        setup: () => {
          if (!isCached) {
            clearLayoutCache()
          }
        },
        run: async () => {
          mounted = mountProfiler(CIRCUIT)
          await mounted.loaded
        },
        teardown: () => {
          nodeCount = mounted!.cy().nodes().length
          expect(mounted!.reported.errors).toEqual([])
          mounted!.unmount()
        }
      })
    results.push(
      await load('load diagram (mount until first layout is painted)', false),
      await load('load diagram, cached layout (mount until first layout is painted)', true)
    )
    expect(nodeCount).toBeGreaterThan(CIRCUIT.profile.graph.nodes.nodes.length)
  })

  it('measures expanding and collapsing a region', async () => {
    const mounted = mountProfiler(CIRCUIT)
    await mounted.loaded
    const region = () => mounted.cy().$id(CIRCUIT.largestRegionId)
    const toggle = async () => {
      const painted = mounted.layoutPainted()
      region().emit('dblclick')
      await painted
    }
    // Each round expands the region, collapses it again, and times both toggles. The layout cache
    // holds the layouts of the earlier rounds, so the ELK rounds clear the cache before each toggle.
    const toggleRounds = async (isCached: boolean) => {
      const expandMs: number[] = []
      const collapseMs: number[] = []
      for (let round = 0; round < REGION_WARMUPS + REGION_RUNS; round++) {
        if (!isCached) {
          clearLayoutCache()
        }
        const expand = await timeAction(toggle)
        expect(region().children().length).toBeGreaterThan(0)
        if (!isCached) {
          clearLayoutCache()
        }
        const collapse = await timeAction(toggle)
        if (round >= REGION_WARMUPS) {
          expandMs.push(expand)
          collapseMs.push(collapse)
        }
      }
      return { expandMs, collapseMs }
    }
    const elk = await toggleRounds(false)
    const cached = await toggleRounds(true)
    results.push(
      summarize('expand region (double click until layout is painted)', elk.expandMs),
      summarize('collapse region (double click until layout is painted)', elk.collapseMs),
      summarize(
        'expand region, cached layout (double click until layout is painted)',
        cached.expandMs
      ),
      summarize(
        'collapse region, cached layout (double click until layout is painted)',
        cached.collapseMs
      )
    )
    expect(region().children().length).toBe(0)
    expect(mounted.reported.errors).toEqual([])
    mounted.unmount()
  })

  it('measures the hover highlight of a node', async () => {
    const mounted = mountProfiler(CIRCUIT)
    await mounted.loaded
    results.push(await measureHover(mounted, 'hover node (mousemove until highlight is painted)'))
    mounted.unmount()
  })

  it('measures one frame of the zoomed-out diagram', async () => {
    const mounted = mountProfiler(CIRCUIT)
    await mounted.loaded
    const cy = mounted.cy()
    const openZoom: number = cy.zoom()
    // At fit view, all nodes are on screen. A pan by one pixel makes cytoscape draw a frame. The
    // direction alternates, so the view stays in place.
    // When no element changed in the last 250 ms, cytoscape draws a frame from a cache of a few large
    // images. Any change, such as a hover, disables that cache for 250 ms, and then each frame
    // draws every node. `skipLayerCache` disables the cache before each frame, so that each frame
    // measures the full draw. `skipLayerCache` uses internal data of the cytoscape 3.33 renderer,
    // which the cytoscape documentation does not describe.
    const skipLayerCache = () => {
      cy.renderer().data.lyrTxrCache.lastInvalidationTime = performance.now()
    }
    const measureFitViewFrame = async (name: string) => {
      cy.fit()
      await afterNextPaint()
      let pans = 0
      return measureFrames(name, {
        warmups: 3,
        runs: 21,
        change: () => {
          skipLayerCache()
          cy.panBy({ x: pans++ % 2 ? 1 : -1, y: 0 })
        }
      })
    }
    results.push(await measureFitViewFrame('frame at fit view, collapsed (pan 1 px)'))

    // Only top-level regions toggle, and an expansion also expands the regions inside. Each
    // expansion runs a layout of the full diagram, so the test expands only some regions to keep the
    // suite short.
    const regionsToExpand: string[] = cy
      .nodes()
      .filter((n: { data(key: string): unknown }) => n.data('has_children') && !n.data('expanded'))
      .map((n: { id(): string }) => n.id())
      .slice(0, EXPANDED_REGIONS)
    for (const id of regionsToExpand) {
      const painted = mounted.layoutPainted()
      cy.$id(id).emit('dblclick')
      await painted
    }
    expect(regionsToExpand.every((id) => cy.$id(id).data('expanded'))).toBe(true)
    results.push(
      await measureFitViewFrame(
        `frame at fit view, ${EXPANDED_REGIONS} regions expanded (pan 1 px)`
      )
    )
    // The expanded diagram draws more edges than the collapsed diagram, so the hover trace colors
    // more edges.
    cy.zoom(openZoom)
    results.push(
      await measureHover(
        mounted,
        `hover node, ${EXPANDED_REGIONS} regions expanded (mousemove until highlight is painted)`
      ),
      await measureMetricSwitch(
        mounted,
        `switch metric, ${EXPANDED_REGIONS} regions expanded (selectMetric until repaint)`
      ),
      await measureWorkerToggle(
        mounted,
        `toggle worker, ${EXPANDED_REGIONS} regions expanded (toggleWorker until repaint)`
      )
    )
    expect(mounted.reported.errors).toEqual([])
    mounted.unmount()
  })

  it('measures switching the selected metric', async () => {
    const mounted = mountProfiler(CIRCUIT)
    await mounted.loaded
    results.push(await measureMetricSwitch(mounted, 'switch metric (selectMetric until repaint)'))
    mounted.unmount()
  })
})
