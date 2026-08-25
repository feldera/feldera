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
