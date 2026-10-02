// Browser tests for what a click on the diagram reports to the application. profiler-lib decides this,
// but the tests need real mouse events on a real canvas: `chipButtons.ts` finds the chip under the
// pointer itself, and cytoscape finds an expanded region under the pointer anywhere inside the region.
// `reported` in `test-support/mountDiagram.ts` records what reached the application.

import { describe, expect, it } from 'vitest'
import { colorDistance, mountDiagram, settle, WITH_SOURCE } from '../test-support/mountDiagram.js'

/** A region `outer` that holds an operator and a region `sub`, which holds one operator. In a
 *  circuit this small, all regions are expanded at the start. */
const NESTED = {
  metrics: [],
  worker_profiles: [{ metadata: {} }],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [
        {
          Cluster: {
            id: 'outer',
            label: 'outer',
            nodes: [
              { Simple: { id: 'n0', label: 'filter' } },
              { Cluster: { id: 'sub', label: 'sub', nodes: [{ Simple: { id: 'n1', label: 'map' } }] } }
            ]
          }
        }
      ]
    },
    edges: []
  }
}

const mount = (keepOpeningView = false) =>
  mountDiagram('light', WITH_SOURCE.profile, keepOpeningView, WITH_SOURCE.dataflow)

/** A rendered point inside the code chip, which is above the top edge of the node: half a chip
 *  height above that edge, and half a chip width in from the right edge. */
// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const codePoint = (cy: any, id: string) => {
  const node = cy.$id(id)
  const { x, y } = node.renderedPosition()
  const zoom = cy.zoom()
  return {
    x: x + node.renderedOuterWidth() / 2 - 15 * zoom,
    y: y - node.renderedOuterHeight() / 2 - 8 * zoom
  }
}

/** A rendered point in the padding at the left side of a region: inside the region, but outside all
 *  the nodes in it and far from its chips. */
// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const regionPoint = (cy: any, id: string) => {
  const node = cy.$id(id)
  const { x, y } = node.renderedPosition()
  return { x: x - node.renderedOuterWidth() / 2 + 5 * cy.zoom(), y }
}

/** Press the mouse button at `from`, move the mouse `by` pixels in two steps, and release the
 *  button. */
const drag = async (
  pointer: (type: 'mousemove' | 'mousedown' | 'mouseup', x: number, y: number) => void,
  from: { x: number, y: number },
  by: { x: number, y: number }
) => {
  pointer('mousemove', from.x, from.y)
  pointer('mousedown', from.x, from.y)
  pointer('mousemove', from.x + by.x / 2, from.y + by.y / 2)
  pointer('mousemove', from.x + by.x, from.y + by.y)
  pointer('mouseup', from.x + by.x, from.y + by.y)
  await settle()
}

// biome-ignore lint/suspicious/noExplicitAny: the cytoscape instance the harness hands back
const highlighted = (cy: any) =>
  cy.edges('.highlight-forward, .highlight-backward').map((e: { id(): string }) => e.id())

describe('a click on a corner chip', () => {
  it('reports on the node the chip belongs to, not on what is behind it', async () => {
    // The code chip is above the top edge of its node, outside the node shape that cytoscape uses. So
    // cytoscape would get the click on the region around the node, and report the region while the
    // source of the node opens.
    const { cy, click, reported, cleanup } = await mount()
    expect(cy.$id('n1').data('has_source')).toBe(true)
    expect(cy.$id('region').isParent()).toBe(true)
    cy.center(cy.$id('n1'))
    await settle()

    const chip = codePoint(cy, 'n1')
    await click(chip.x, chip.y)
    // A click on the code chip asks for the source of the node. The application gets this as the same
    // event that a double click on an operator sends.
    expect(reported.doubleClicks).toEqual([{ nodeId: 'n1', type: 'leaf' }])
    // The click on the chip is also a click on its node: the node is reported, marked and traced.
    expect(reported.nodeClicks).toEqual(['n1'])
    expect(reported.attributes.filter((a) => a.isSticky)).toEqual([
      { nodeId: 'n1', isSticky: true }
    ])
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    expect(highlighted(cy).length).toBeGreaterThan(0)
    cleanup()
  })

  it('expands or collapses the region on a counter click, and reports nothing', async () => {
    // A click on the counter expands or collapses the circuit region, and then the graph is made again.
    // A report would be out of date one frame later, so the click reports nothing.
    const { cy, click, reported, cleanup } = await mount()
    const region = cy.$id('region')
    cy.center(region)
    await settle()

    const { x, y } = region.renderedPosition()
    const counter = {
      x: x + region.renderedOuterWidth() / 2 - 6 * cy.zoom(),
      y: y - region.renderedOuterHeight() / 2 + 8 * cy.zoom()
    }
    await click(counter.x, counter.y)
    expect(cy.$id('region').isParent()).toBe(false)
    expect(reported.nodeClicks).toEqual([])
    expect(reported.attributes.filter((a) => a.isSticky)).toEqual([])
    cleanup()
  })

  it('does not paint the cytoscape press overlay on the node under the chip', async () => {
    // On `mousedown`, cytoscape makes the node under the pointer active and paints a gray overlay on
    // all of it. For a code chip in the top band of a region, that is the whole region, and the user
    // sees the overlay while the button is down.
    const { cy, pixelAt, pointer, reported, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()
    // A point in the padding of the region: inside the gray overlay, and outside all nodes in the
    // region.
    const region = cy.$id('region')
    const band = {
      x: region.renderedPosition().x - region.renderedOuterWidth() / 2 + 5 * cy.zoom(),
      y: region.renderedPosition().y
    }
    const before = pixelAt(band.x, band.y)

    const chip = codePoint(cy, 'n1')
    pointer('mousemove', chip.x, chip.y)
    pointer('mousedown', chip.x, chip.y)
    await settle()
    expect(cy.nodes(':active').map((n: { id(): string }) => n.id())).toEqual([])
    expect(colorDistance(pixelAt(band.x, band.y), before)).toBe(0)

    // Check that the click still works: the `mouseup` clicks the chip. Otherwise the checks above could
    // pass only because nothing reacts to the `mousedown`.
    pointer('mouseup', chip.x, chip.y)
    await settle()
    expect(reported.nodeClicks).toEqual(['n1'])
    cleanup()
  })

  it('does not pan the view when a chip over the background is dragged', async () => {
    // The code chip of a node that is not in a region is over the empty canvas. Cytoscape treats a
    // `mousedown` there as a press on the background: it shows a gray dot, and when the mouse moves, it
    // pans the whole diagram.
    const { cy, pointer, reported, cleanup } = await mount()
    expect(cy.$id('n0').data('has_source')).toBe(true)
    expect(cy.$id('n0').isChild()).toBe(false)
    cy.center(cy.$id('n0'))
    await settle()
    const pan = { ...cy.pan() }

    await drag(pointer, codePoint(cy, 'n0'), { x: 90, y: 40 })
    expect(cy.pan()).toEqual(pan)
    // As with any button, a drag off the chip cancels the click.
    expect(reported.doubleClicks).toEqual([])
    expect(reported.nodeClicks).toEqual([])
    cleanup()
  })

  it('does not drag the node under the chip', async () => {
    // At `mousedown`, cytoscape uses the node shape to decide what a drag moves. So a drag that starts
    // on a code chip would move the region under it, and all nodes in the region, away from their
    // layout positions.
    const { cy, pointer, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()
    const before = { region: { ...cy.$id('region').position() }, n1: { ...cy.$id('n1').position() } }

    await drag(pointer, codePoint(cy, 'n1'), { x: 80, y: 60 })
    expect(cy.$id('region').position()).toEqual(before.region)
    expect(cy.$id('n1').position()).toEqual(before.n1)

    // A drag on the body of a node still moves the node. Only a drag that starts on a chip is hidden
    // from cytoscape.
    const outside = cy.$id('n0')
    const start = { ...outside.position() }
    await drag(pointer, outside.renderedPosition(), { x: 40, y: 30 })
    expect(outside.position()).not.toEqual(start)
    cleanup()
  })

  it('does not select the node under the chip', async () => {
    // The diagram does not use or show the cytoscape selection, but cytoscape uses it to decide what a
    // later drag moves. So a click on a chip must not select the region either.
    const { cy, click, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()

    const chip = codePoint(cy, 'n1')
    await click(chip.x, chip.y)
    expect(cy.$('node:selected').map((n: { id(): string }) => n.id())).toEqual([])

    // A click on the region itself still selects it, as a click on any node does.
    const point = regionPoint(cy, 'region')
    await click(point.x, point.y)
    expect(cy.$('node:selected').map((n: { id(): string }) => n.id())).toEqual(['region'])
    cleanup()
  })

  it('does not collapse the region under the chip on a double click', async () => {
    // Cytoscape reports two quick clicks as a double click on the node under the chip, and a double
    // click on a region collapses it.
    const { cy, click, reported, cleanup } = await mount()
    cy.center(cy.$id('n1'))
    await settle()

    const chip = codePoint(cy, 'n1')
    await click(chip.x, chip.y)
    await click(chip.x, chip.y)
    await settle()
    expect(cy.$id('region').isParent()).toBe(true)
    expect(reported.doubleClicks).toEqual([
      { nodeId: 'n1', type: 'leaf' },
      { nodeId: 'n1', type: 'leaf' }
    ])
    cleanup()
  })
})

describe('a click on an expanded region', () => {
  it('reports the metrics of the region', async () => {
    // A region holds the total metrics of all nodes in it, and a click on the region shows them. The
    // check that stops a hover from reporting each region that the pointer moves across must not also
    // block the click.
    const { cy, click, reported, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    await click(point.x, point.y)

    expect(reported.nodeClicks).toEqual(['region'])
    expect(reported.attributes.filter((a) => a.isSticky)).toEqual([
      { nodeId: 'region', isSticky: true }
    ])
    cleanup()
  })

  it('marks nothing and colors no edges, unlike a click on an operator', async () => {
    // A region contains many nodes, so a trace from it would color all edges in it. Also, a region
    // cannot show the mark, because a region never glows.
    const { cy, click, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    await click(point.x, point.y)
    expect(cy.nodes('.selected-node').length).toBe(0)
    expect(highlighted(cy)).toEqual([])

    // Compare with a click on the operator inside the region: the click marks the operator and traces
    // its edges.
    const node = cy.$id('n1')
    await click(node.renderedPosition().x, node.renderedPosition().y)
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    expect(highlighted(cy).length).toBeGreaterThan(0)
    cleanup()
  })

  it('keeps its report when the pointer then moves over an operator', async () => {
    // A hover and a click both show their report in the same place. The only difference is that a
    // report from a click stays until the user closes it.
    const { cy, click, reported, cleanup } = await mount(true)
    const node = cy.$id('n1')
    node.emit('mouseover')
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'n1', isSticky: false })

    const point = regionPoint(cy, 'region')
    await click(point.x, point.y)
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })

    // A hover on an operator after the click does not replace the region report.
    node.emit('mouseover')
    await settle()
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })
    cleanup()
  })

  it('still lets a hover mark and trace operators', async () => {
    // The region report does not mark a node or color edges, so the diagram shows nothing for it. If
    // the report also blocked hovers, the graph would stop reacting to the pointer, and the click would
    // look like it did nothing.
    const { cy, click, reported, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    await click(point.x, point.y)
    expect(cy.nodes('.selected-node').length).toBe(0)

    const node = cy.$id('n1')
    node.emit('mouseover')
    await settle()
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    expect(highlighted(cy).length).toBeGreaterThan(0)
    // The hover does not change the report from the click, which stays until the user closes it.
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })

    // The mark goes away with the pointer, but the report stays.
    node.emit('mouseout')
    await settle()
    expect(cy.nodes('.selected-node').length).toBe(0)
    expect(highlighted(cy)).toEqual([])
    expect(reported.attributes.at(-1)).toEqual({ nodeId: 'region', isSticky: true })
    cleanup()
  })

  it('keeps the mark of a clicked operator when the pointer moves to another node', async () => {
    // The opposite case: a report that marks a node keeps that mark during hovers. Otherwise a click on
    // an operator would mark it only until the pointer moved to another node.
    const { cy, click, cleanup } = await mount(true)
    const node = cy.$id('n1')
    await click(node.renderedPosition().x, node.renderedPosition().y)
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])

    cy.$id('n2').emit('mouseover')
    await settle()
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    cleanup()
  })

  it('reports nothing when the pointer only moves across it', async () => {
    // This is why the check exists: a region covers all nodes in it, so without the check, the user
    // would see a region report each time the pointer moved into a region on the way to a node.
    const { cy, pointer, reported, cleanup } = await mount(true)
    const point = regionPoint(cy, 'region')
    pointer('mousemove', point.x, point.y)
    cy.$id('region').emit('mouseover')
    await settle()
    expect(reported.attributes.filter((a) => a.nodeId !== null)).toEqual([])
    cleanup()
  })
})

describe('the overview report a profile opens with', () => {
  it('still lets a hover mark and trace operators', async () => {
    // The overview is a sticky report on the root node. The root node is invisible, so it has no mark.
    // The application shows the overview when a profile loads. If the overview blocked hovers, the
    // diagram would not react to the pointer until the user closed a report that they did not ask for.
    const { cy, diagram, reported, cleanup } = await mount(true)
    diagram.showGlobalMetrics(true)
    await settle()
    expect(cy.nodes('.selected-node').length).toBe(0)

    const node = cy.$id('n1')
    node.emit('mouseover')
    await settle()
    expect(cy.nodes('.selected-node').map((n: { id(): string }) => n.id())).toEqual(['n1'])
    expect(highlighted(cy).length).toBeGreaterThan(0)

    // The mark goes away with the pointer, but the overview stays, because it is a sticky report.
    node.emit('mouseout')
    await settle()
    expect(cy.nodes('.selected-node').length).toBe(0)
    expect(highlighted(cy)).toEqual([])
    expect(reported.attributes.at(-1)?.isSticky).toBe(true)
    cleanup()
  })
})

describe('a double click on a nested region', () => {
  it('does nothing, since only the top-level region around it expands and collapses', async () => {
    const { cy, toggle, reported, cleanup } = await mountDiagram('light', NESTED)
    expect(cy.$id('outer').isParent()).toBe(true)
    expect(cy.$id('sub').isParent()).toBe(true)

    await toggle('sub')
    expect(cy.$id('outer').isParent()).toBe(true)
    expect(cy.$id('sub').isParent()).toBe(true)
    expect(reported.doubleClicks).toEqual([])

    // The top-level region still collapses, and the nested region is no longer shown.
    await toggle('outer')
    expect(cy.$id('outer').isParent()).toBe(false)
    expect(cy.$id('sub').length).toBe(0)
    expect(reported.doubleClicks).toEqual([{ nodeId: 'outer', type: 'group' }])
    cleanup()
  })
})
