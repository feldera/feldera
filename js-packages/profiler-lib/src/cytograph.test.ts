import { describe, expect, it, vi } from 'vitest'

// `cytograph.ts` registers the dblclick extension at import time, which touches `window`. The node
// test environment has none, and node definitions are plain data - no renderer involved.
vi.mock('cytoscape-dblclick', () => ({ default: () => {} }))

import { Cytograph } from './cytograph.js'
import { labelWidth } from './diagramTheme.js'
import { regionMinWidth } from './regionSize.js'
import { CircuitProfile } from './profile.js'
import { CircuitSelection } from './selection.js'
import { ExplicitSubSet } from './util.js'

const simple = (id: string, label: string) => ({ Simple: { id, label } })
const cluster = (id: string, label: string, nodes: unknown[]) => ({ Cluster: { id, label, nodes } })

const profile = () =>
    CircuitProfile.fromJson({
        metrics: [],
        worker_profiles: [{ metadata: {} }],
        graph: {
            nodes: {
                id: 'n',
                label: 'circuit',
                nodes: [simple('n1', 'source'), cluster('region', 'region', [simple('n2', 'map')])]
            },
            edges: []
        }
    } as never).profile

/** A region inside a region, the case a single level of expansion cannot describe. */
const nested = () =>
    CircuitProfile.fromJson({
        metrics: [],
        worker_profiles: [{ metadata: {} }],
        graph: {
            nodes: {
                id: 'n',
                label: 'circuit',
                nodes: [
                    cluster('outer', 'outer', [
                        simple('n1', 'source'),
                        cluster('inner', 'inner', [simple('n2', 'map')])
                    ])
                ]
            },
            edges: [{ from_node: 'n1', to_node: 'n2' }]
        }
    } as never).profile

/** The nodes of the graph drawn for `profile` with exactly `expanded` expanded, by id. */
const drawn = (p: CircuitProfile, expanded: string[]) => {
    const selection = new CircuitSelection(
        new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set(expanded))
    )
    const elements = Cytograph.fromProfile(p, selection).getGraphElements('light')
    return new Map(elements.nodes.map((n) => [n.data.id as string, n.data]))
}

/** Nothing expanded, so `region` is drawn as one collapsed node. */
const collapsedGraph = () => {
    const p = profile()
    const selection = new CircuitSelection(
        new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set())
    )
    return Cytograph.fromProfile(p, selection).getGraphElements('light')
}

describe('node definitions', () => {
    it('marks a collapsed composite as such at insert time', () => {
        // The stylesheet gives composites their own corner radius via `node[?has_children]`. That
        // has to be part of the node definition: an attribute written only by a later graph update
        // would leave the first paint styled as a plain operator.
        const nodes = collapsedGraph().nodes
        const byId = new Map(nodes.map((n) => [n.data.id, n.data]))
        expect(byId.get('region')!.has_children).toBe(true)
        expect(byId.get('n1')!.has_children).toBe(false)
    })

    it('carries what the counter chip reports and hit-tests by', () => {
        // `chipButtons.ts` sizes the counter pill from the count in node data, which is also what the
        // chip images are built from - so the number has to be on the node, not only inside the image.
        const byId = new Map(collapsedGraph().nodes.map((n) => [n.data.id, n.data]))
        expect(byId.get('region')!.leaf_count).toBe(1)
        expect(byId.get('n1')!.leaf_count).toBe(0)
    })

    it('carries the text both as one label to measure and as the operator name to draw', () => {
        // `label` is only measured, to size the node; `nodeText.ts` draws the id and the operator as
        // two runs, so the operator name has to be reachable on its own.
        const nodes = collapsedGraph().nodes
        const byId = new Map(nodes.map((n) => [n.data.id, n.data]))
        expect(byId.get('n1')!.label).toBe('n1 source')
        expect(byId.get('n1')!.operator).toBe('source')
        expect(byId.get('region')!.label).toBe('region region')
        expect(byId.get('region')!.operator).toBe('region')
    })

    it('gives a composite a floor under the width it is drawn at while expanded', () => {
        // An expanded region is sized by the nodes it holds, so the room its own name needs has to
        // travel with the node definition: see `regionSize.ts`. An operator is sized by its text
        // directly and needs no floor.
        const byId = new Map(collapsedGraph().nodes.map((n) => [n.data.id, n.data]))
        expect(byId.get('region')!.min_width).toBe(regionMinWidth('region region', 1))
        expect(byId.get('n1')!.min_width).toBe(0)
    })

    it('sizes every node from its own measured text', () => {
        // Measured here rather than by cytoscape's `width: 'label'`, which it deprecates and which
        // leaves the width outside this module's hands.
        const byId = new Map(collapsedGraph().nodes.map((n) => [n.data.id, n.data]))
        for (const id of ['n1', 'region']) {
            expect(byId.get(id)!.text_width, id).toBe(labelWidth(byId.get(id)!.label))
        }
    })
})

describe('nested regions', () => {
    it('draws a collapsed region inside an expanded one', () => {
        // Why the regions are walked from the outside in: consult only the outermost one and expanding
        // it reveals every descendant, leaving `inner` impossible to collapse on its own.
        const nodes = drawn(nested(), ['outer'])
        expect([...nodes.keys()].sort()).toEqual(['inner', 'n', 'n1', 'outer'])
        expect(nodes.get('inner')!.has_children).toBe(true)
        expect(nodes.get('inner')!.parent).toBe('outer')
        // `inner` stands in for what it hides, so it reports the count of it.
        expect(nodes.get('inner')!.leaf_count).toBe(1)
    })

    it('draws every operator once both regions are expanded', () => {
        const nodes = drawn(nested(), ['outer', 'inner'])
        expect([...nodes.keys()].sort()).toEqual(['inner', 'n', 'n1', 'n2', 'outer'])
        // Both regions are on the graph, since cytoscape needs every `parent` it is given to exist.
        expect(nodes.get('n2')!.parent).toBe('inner')
        expect(nodes.get('inner')!.parent).toBe('outer')
    })

    it('hides an expanded region inside a collapsed one, there being nowhere to draw it', () => {
        const nodes = drawn(nested(), ['inner'])
        expect([...nodes.keys()].sort()).toEqual(['n', 'outer'])
        expect(nodes.get('outer')!.leaf_count).toBe(2)
    })

    it('lands an edge on whichever region is drawn in place of its endpoints', () => {
        const elements = (expanded: string[]) => {
            const p = nested()
            const selection = new CircuitSelection(
                new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set(expanded))
            )
            return Cytograph.fromProfile(p, selection).getGraphElements('light').edges
        }
        // n1 -> n2, with n2 hidden inside the collapsed `inner`.
        const collapsed = elements(['outer'])
        expect(collapsed).toHaveLength(1)
        expect(collapsed[0]!.data.source).toBe('n1')
        expect(collapsed[0]!.data.target).toBe('inner')
        // Both expanded, so the edge is the one the profile describes.
        expect(elements(['outer', 'inner'])[0]!.data.target).toBe('n2')
        // Everything hidden: the edge would be a self-edge on `outer`, so it is not drawn at all.
        expect(elements([])).toHaveLength(0)
    })
})
