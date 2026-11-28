/**
 * Screenshots of the edge labels of the profiler diagram. A label shows the row count of an edge
 * and the stream count below it. The label of a traced edge has a border in the edge color.
 */

import { ProfilerDiagram } from 'profiler-layout'
import { CircuitProfile, type JsonProfiles, type ProfilerCallbacks } from 'profiler-lib'
import { beforeAll, describe, expect, it, onTestFinished, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

/** The output statistics of an operator that wrote `records` rows in one batch on one worker. */
const outputRows = (records: number) => [
  {
    metric_id: 'output_batches_stats',
    value: {
      batches_count: { value: 1 },
      min_records_count: { value: records },
      max_records_count: { value: records },
      avg_records_count: { value: records },
      total_records_count: { value: records }
    }
  }
]

const simple = (id: string, label: string) => ({ Simple: { id, label } })
const edge = (from: string, to: string) => ({ from_node: from, to_node: to })

//   Profile                        Drawn, with `region` collapsed
//
//            | region |              src
//     src ---+-> a ---+-> sink        |  950 rows
//            |   b ---+---^           v
//            |        |             region
//                                     |  1.23k rows, 2 streams
//                                     v
//                                    sink
//
// The edges `a -> sink` and `b -> sink` become one drawn edge that carries two streams.
const PROFILE = {
  metrics: [],
  worker_profiles: [
    { metadata: { src: outputRows(950), a: outputRows(1000), b: outputRows(234) } }
  ],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [
        simple('src', 'source'),
        {
          Cluster: {
            id: 'region',
            label: 'shard',
            nodes: [simple('a', 'map'), simple('b', 'filter')]
          }
        },
        simple('sink', 'sink')
      ]
    },
    edges: [edge('src', 'a'), edge('a', 'sink'), edge('b', 'sink')]
  }
}

// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance that the diagram creates
type Cy = any

/** Waits until `ready` is true, then for one draw of cytoscape that starts after that. The screenshot
 *  then shows the state that `ready` checks. */
const drawnWhen = async (cy: Cy, ready: () => boolean = () => true) => {
  await vi.waitFor(() => expect(ready()).toBe(true))
  const drawn = new Promise((resolve) => cy.one('render', resolve))
  cy.forceRender()
  await drawn
}

/** Mount the diagram over `PROFILE`, wait for its layout, and fit the view to the whole graph. */
const mountDiagram = async () => {
  let laidOut: () => void
  const layout = new Promise<void>((resolve) => (laidOut = resolve))
  const callbacks = {
    displayNodeAttributes: () => {},
    displayTopNodes: () => {},
    onMetricsChanged: () => {},
    displayMessage: () => {},
    onError: (e: string) => {
      throw new Error(e)
    },
    onRenderingChange: (inFlight: boolean) => {
      if (!inFlight) {
        laidOut()
      }
    }
  } satisfies ProfilerCallbacks
  const { profile } = CircuitProfile.fromJson(PROFILE as unknown as JsonProfiles)

  const wrapper = document.createElement('div')
  wrapper.style.cssText = 'position:relative;width:600px;height:400px'
  document.body.appendChild(wrapper)
  onTestFinished(() => wrapper.remove())
  render(ProfilerDiagram, { target: wrapper, props: { profile, callbacks, theme: 'light' } })
  await layout

  const container = wrapper.querySelector('.visualizer-graph') as HTMLElement & {
    _cyreg: { cy: Cy }
  }
  const cy = container._cyreg.cy
  cy.fit(cy.elements(), 20)
  await drawnWhen(cy)
  return { cy, graph: page.elementLocator(container) }
}

describe('ProfilerDiagram edge labels', () => {
  // The browser is slow while the other test files run. Thus, the screenshot can take longer.
  const screenshotOptions = { timeout: 15_000 }
  beforeAll(async () => {
    await page.viewport(1280, 720)
  })

  it('shows the rows of an edge, and the stream count below them', async () => {
    const { cy, graph } = await mountDiagram()
    // The region starts collapsed, so `a` and `b` feed `sink` over one drawn edge.
    expect(
      cy
        .edges()
        .map((e: Cy) => e.data('label'))
        .sort()
    ).toEqual(['1.23k rows\n2 streams', '950 rows'])
    await expect.element(graph).toMatchScreenshot('edge-labels', screenshotOptions)
  })

  it('draws the label border of a traced edge in the color of the edge', async () => {
    const { cy, graph } = await mountDiagram()
    // A `mouseover` on `sink` traces the edges that reach it.
    cy.$id('sink').emit('mouseover')
    // The diagram raises the traced edges one frame after it traces them.
    await drawnWhen(cy, () => cy.edges('.raised-edge').length === 2)
    expect(cy.edges('.highlight-backward')).toHaveLength(2)
    await expect.element(graph).toMatchScreenshot('edge-labels-traced', screenshotOptions)
  })
})
