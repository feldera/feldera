// The profiler performance suite. Run it with `bun run test-perf`, see `vitest.perf.config.ts`.
// Each measurement times the real code path, from the user action until the result is painted.
// The tests only check that each action completed. They do not check a time budget.

import { CircuitProfile, type ProfilerCallbacks } from 'profiler-lib'
import { afterAll, describe, expect, inject, it } from 'vitest'
import { commands } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import ProfilerDiagram from './components/ProfilerDiagram.svelte'
import { getSuitableProfiles, processProfileFiles } from './functions/processZipBundle'
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
/** For the diagram measurements. The ELK layout time grows fast with the number of nodes on screen,
 *  so this circuit is smaller than a large real one, to keep the suite at about half a minute. */
const CIRCUIT = syntheticCircuit({
  seed: 1,
  topLevelItems: inject('perfTopLevelItems'),
  workers: WORKERS
})
/** For the parsing measurements, which do not lay out the circuit: about 2400 operators, the size
 *  of a large real circuit. */
const LARGE_CIRCUIT = syntheticCircuit({ seed: 1, topLevelItems: 200, workers: WORKERS })
const DIAGRAM_SIZE = { width: 1200, height: 800 }
/** Fail an action whose layout never ends, instead of waiting for the test timeout. */
const LAYOUT_TIMEOUT_MS = 60_000
const REGION_WARMUPS = 1
const REGION_RUNS = 9
/** How many top-level regions the frame measurement expands. With the default circuit size, this
 *  makes the diagram about five times as many nodes as when it is collapsed. */
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

/** Mount the diagram over `circuit`, and give helpers that wait for the diagram to finish what a
 *  user action started. */
function mountProfiler(circuit: SyntheticCircuit) {
  const wrapper = document.createElement('div')
  wrapper.style.cssText = `position:relative;width:${DIAGRAM_SIZE.width}px;height:${DIAGRAM_SIZE.height}px`
  document.body.appendChild(wrapper)

  let onLayoutDone: (() => void) | undefined
  const reported = { hoveredNodes: [] as Array<string | null>, errors: [] as string[] }
  const callbacks: ProfilerCallbacks = {
    displayNodeAttributes: (data) => {
      reported.hoveredNodes.push(data.match({ some: (v) => v.nodeId, none: () => null }))
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

  /** Resolve when the next layout ends and its result is painted. Call it before the action that
   *  starts the layout. */
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
      profileData: circuit.profile,
      dataflowData: circuit.dataflow,
      programCode: circuit.programCode,
      callbacks
    }
  })
  const container = wrapper.querySelector('.visualizer-graph') as HTMLElement & {
    // biome-ignore lint/suspicious/noExplicitAny: the registry cytoscape leaves on its container
    _cyreg?: { cy: any }
  }

  /** Move the mouse to a rendered position on the canvas, so that cytoscape finds the node there
   *  itself, as for a real pointer. */
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
      search(query: string): void
    },
    unmount: () => {
      screen.unmount()
      wrapper.remove()
    }
  }
}

/** Time the hover highlight of the source operator, at the current zoom. The source operator is
 *  upstream of most of the circuit, so its highlight colors the most edges. */
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
      // The search above already marked the node, so the report is what shows the hover.
      expect(mounted.reported.hoveredNodes.at(-1)).toBe(CIRCUIT.sourceOperatorId)
      // The bottom right corner is empty, and away from the minimap at the top left.
      mounted.moveMouse(DIAGRAM_SIZE.width - 2, DIAGRAM_SIZE.height - 2)
      await afterNextPaint()
    }
  })
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
          workers = processed.profile!.worker_profiles.length
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
          const circuit = CircuitProfile.fromJson(LARGE_CIRCUIT.profile)
          circuit.profile.setDataflow(LARGE_CIRCUIT.dataflow, LARGE_CIRCUIT.programCode)
          operators = circuit.profile.simpleNodes.size
        }
      })
    )
    expect(operators).toBe(LARGE_CIRCUIT.operatorCount)
  })

  it('measures the first load of the diagram', async () => {
    let mounted: ReturnType<typeof mountProfiler> | undefined
    let nodeCount = 0
    results.push(
      await measure('load diagram (mount until first layout is painted)', {
        warmups: 1,
        runs: 5,
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
    // Each round expands the region and collapses it again, and times both.
    const expandMs: number[] = []
    const collapseMs: number[] = []
    let childrenWhenExpanded = 0
    for (let round = 0; round < REGION_WARMUPS + REGION_RUNS; round++) {
      const expand = await timeAction(toggle)
      childrenWhenExpanded = region().children().length
      const collapse = await timeAction(toggle)
      if (round >= REGION_WARMUPS) {
        expandMs.push(expand)
        collapseMs.push(collapse)
      }
    }
    results.push(
      summarize('expand region (double click until layout is painted)', expandMs),
      summarize('collapse region (double click until layout is painted)', collapseMs)
    )
    expect(childrenWhenExpanded).toBeGreaterThan(0)
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
    // images. Any change, such as a hover, turns that cache off for 250 ms, and then each frame draws
    // every node. This function turns the cache off, so that each frame measures the full draw. It
    // uses the renderer of cytoscape 3.33, which is not in the cytoscape documentation.
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

    // Only top-level regions toggle, and the regions inside them expand with them. Each expansion
    // runs a layout of the full diagram, so the test expands only some regions, to keep the suite
    // short.
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
    // More edges are drawn than in the collapsed diagram, so the hover colors more edges.
    cy.zoom(openZoom)
    results.push(
      await measureHover(
        mounted,
        `hover node, ${EXPANDED_REGIONS} regions expanded (mousemove until highlight is painted)`
      )
    )
    expect(mounted.reported.errors).toEqual([])
    mounted.unmount()
  })

  it('measures switching the selected metric', async () => {
    const mounted = mountProfiler(CIRCUIT)
    await mounted.loaded
    const metrics = ['runtime_percent', 'used_memory_bytes']
    const shading = () =>
      mounted
        .cy()
        .nodes()
        .map((n: { data(key: string): unknown }) => n.data('value'))
    let switches = 0
    results.push(
      await measure('switch metric (selectMetric until repaint)', {
        warmups: 3,
        runs: 15,
        run: async () => {
          switches++
          mounted.diagram.selectMetric(metrics[switches % 2]!)
          await afterNextPaint()
        }
      })
    )
    // The node shades follow the metric, so a switch that took effect changes them.
    const before = JSON.stringify(shading())
    mounted.diagram.selectMetric(metrics[(switches + 1) % 2]!)
    expect(JSON.stringify(shading())).not.toBe(before)
    mounted.unmount()
  })
})
