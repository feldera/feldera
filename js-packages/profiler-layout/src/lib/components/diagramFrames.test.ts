// Browser tests for the frames that cytoscape draws while a layout runs. The diagram stops the frames
// until the layout is finished (see `pauseFrames` in profiler-lib). This depends on cytoscape renderer
// internals, so these tests fail if a cytoscape update breaks it.

import { describe, expect, it } from 'vitest'
import { mountDiagram, settle } from '../test-support/mountDiagram.js'

const simple = (id: string, label: string) => ({ Simple: { id, label } })

/** A region with a chain of 150 operators. ELK needs more than one animation frame to lay it out, so
 *  cytoscape has the time to draw a frame during the layout. */
const LARGE_REGION = {
  metrics: [],
  worker_profiles: [{ metadata: {} }],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [
        {
          Cluster: {
            id: 'region',
            label: 'shard',
            nodes: Array.from({ length: 150 }, (_, i) => simple(`n${i}`, `operator ${i}`))
          }
        }
      ]
    },
    edges: Array.from({ length: 149 }, (_, i) => ({ from_node: `n${i}`, to_node: `n${i + 1}` }))
  }
}

// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
type Cy = any

/** Expand the region, call `during` after the graph changed and the layout started, and resolve when
 *  the layout is finished. Resolves with the number of frames that cytoscape drew during the layout.
 *  The toggle changes the graph in a later task, after the progress message is on screen. The same
 *  task adds the new nodes and starts the layout. */
const expandRegion = (cy: Cy, during: () => void = () => {}) =>
  new Promise<number>((resolve) => {
    let frames = 0
    const onRender = () => frames++
    cy.one('add', () => {
      cy.on('render', onRender)
      queueMicrotask(during)
    })
    cy.one('layoutstop', () => {
      cy.off('render', onRender)
      resolve(frames)
    })
    cy.$id('region').emit('dblclick')
  })

describe('a running layout', () => {
  it('draws no frame until the layout is finished, then draws the new layout', async () => {
    const { cy, nodeFill, cleanup } = await mountDiagram('light', LARGE_REGION)
    expect(await expandRegion(cy)).toBe(0)
    let frames = 0
    cy.on('render', () => frames++)
    await settle()
    expect(frames).toBeGreaterThan(0)
    expect(nodeFill('n0').a).toBe(1)
    cleanup()
  })

  it('shows a theme change that happens during the layout', async () => {
    const { cy, diagram, nodeFill, cleanup } = await mountDiagram('light', LARGE_REGION)
    await expandRegion(cy, () => {
      document.documentElement.dataset.theme = 'dark'
      diagram.setTheme('dark')
    })
    await settle()
    // A node of the light theme is almost white.
    const { r, g, b, a } = nodeFill('n0')
    expect(a).toBe(1)
    expect(r + g + b).toBeLessThan(3 * 128)
    cleanup()
  })

  it('shows a resize that happens during the layout', async () => {
    // A resize clears the canvas, so only a new frame can show the node.
    const { cy, container, nodeFill, cleanup } = await mountDiagram('light', LARGE_REGION)
    const width = container.clientWidth
    await expandRegion(cy, () => {
      container.style.width = `${width / 2}px`
      cy.resize()
    })
    await settle()
    expect(cy.width()).toBe(Math.round(width / 2))
    cy.center(cy.$id('n0'))
    await settle()
    expect(nodeFill('n0').a).toBe(1)
    cleanup()
  })

  it('ends without an error when the diagram is unmounted during the layout', async () => {
    const { cy, unmount } = await mountDiagram('light', LARGE_REGION)
    const errors: string[] = []
    const onError = (event: ErrorEvent) => {
      errors.push(event.message)
      event.preventDefault()
    }
    window.addEventListener('error', onError)
    cy.$id('region').emit('dblclick')
    unmount()
    await settle()
    window.removeEventListener('error', onError)
    expect(errors).toEqual([])
  })
})
