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
    it('marks a collapsed circuit region as such at insert time', () => {
        // The stylesheet gives circuit regions their own corner radius via `node[?has_children]`. That
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

    it('gives a circuit region a floor under the width it is drawn at while expanded', () => {
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

describe('collapsed regions', () => {
    it('lists all the operators inside a collapsed region', () => {
        // The hover trace of a collapsed region starts from these operators.
        const p = CircuitProfile.fromJson({
            metrics: [],
            worker_profiles: [{ metadata: {} }],
            graph: {
                nodes: {
                    id: 'n',
                    label: 'circuit',
                    nodes: [cluster('region', 'region', [simple('n2', 'map'), simple('n3', 'filter')])]
                },
                edges: []
            }
        } as never).profile
        const selection = new CircuitSelection(
            new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set())
        )
        const graph = Cytograph.fromProfile(p, selection)
        expect([...graph.getSimpleNodes('region')].sort()).toEqual(['n2', 'n3'])
    })
})

describe('drawn edges', () => {
    // One batch of `records` rows written by an operator on one worker.
    const outputRows = (records: number) => [{
        metric_id: 'output_batches_stats',
        value: {
            batches_count: { value: 1 },
            min_records_count: { value: records },
            max_records_count: { value: records },
            avg_records_count: { value: records },
            total_records_count: { value: records }
        }
    }]

    it('draws one edge for the parallel edges between two drawn nodes', () => {
        // Both operators in the collapsed region feed n1, so two edges of the circuit become one drawn
        // edge. The hover trace finds the drawn edge from either edge of the circuit.
        const p = CircuitProfile.fromJson({
            metrics: [],
            worker_profiles: [{ metadata: {} }],
            graph: {
                nodes: {
                    id: 'n',
                    label: 'circuit',
                    nodes: [
                        simple('n1', 'sink'),
                        cluster('region', 'region', [simple('n2', 'map'), simple('n3', 'filter')])
                    ]
                },
                edges: [
                    { from_node: 'n2', to_node: 'n1' },
                    { from_node: 'n3', to_node: 'n1' }
                ]
            }
        } as never).profile
        const selection = new CircuitSelection(
            new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set())
        )
        const graph = Cytograph.fromProfile(p, selection)
        expect(graph.edges.map((e) => e.getId())).toEqual(['region->n1'])
        expect([...graph.edgeMap.entries()].sort()).toEqual([
            ['n2->n1', 'region->n1'],
            ['n3->n1', 'region->n1']
        ])
        // The two edges come from two operators, so they carry two streams.
        expect(graph.edges[0]!.getDefinition().data.label).toBe('2 streams')
    })

    it('shows the rows of the streams over the stream count', () => {
        const p = CircuitProfile.fromJson({
            metrics: [],
            worker_profiles: [{ metadata: { n2: outputRows(1000), n3: outputRows(234) } }],
            graph: {
                nodes: {
                    id: 'n',
                    label: 'circuit',
                    nodes: [
                        simple('n1', 'sink'),
                        cluster('region', 'region', [simple('n2', 'map'), simple('n3', 'filter')])
                    ]
                },
                edges: [
                    { from_node: 'n2', to_node: 'n1' },
                    { from_node: 'n3', to_node: 'n1' }
                ]
            }
        } as never).profile
        const selection = new CircuitSelection(
            new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set())
        )
        const graph = Cytograph.fromProfile(p, selection)
        expect(graph.edges[0]!.getDefinition().data.label).toBe('1.23k rows\n2 streams')
    })

    it('shows no count for one stream that feeds several operators of a collapsed region', () => {
        // n1 writes one stream, and both operators in the region read it.
        const p = CircuitProfile.fromJson({
            metrics: [],
            worker_profiles: [{ metadata: {} }],
            graph: {
                nodes: {
                    id: 'n',
                    label: 'circuit',
                    nodes: [
                        simple('n1', 'source'),
                        cluster('region', 'region', [simple('n2', 'map'), simple('n3', 'filter')])
                    ]
                },
                edges: [
                    { from_node: 'n1', to_node: 'n2' },
                    { from_node: 'n1', to_node: 'n3' }
                ]
            }
        } as never).profile
        const selection = new CircuitSelection(
            new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set())
        )
        const graph = Cytograph.fromProfile(p, selection)
        expect(graph.edges.map((e) => e.getId())).toEqual(['n1->region'])
        expect(graph.edges[0]!.getDefinition().data.label).toBeUndefined()
    })

    it('counts the rows of one stream once, although it feeds several operators of a region', () => {
        // n1 writes 950 rows, and both operators in the collapsed region read them.
        const p = CircuitProfile.fromJson({
            metrics: [],
            worker_profiles: [{ metadata: { n1: outputRows(950) } }],
            graph: {
                nodes: {
                    id: 'n',
                    label: 'circuit',
                    nodes: [
                        simple('n1', 'source'),
                        cluster('region', 'region', [simple('n2', 'map'), simple('n3', 'filter')])
                    ]
                },
                edges: [
                    { from_node: 'n1', to_node: 'n2' },
                    { from_node: 'n1', to_node: 'n3' }
                ]
            }
        } as never).profile
        const selection = new CircuitSelection(
            new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set())
        )
        const graph = Cytograph.fromProfile(p, selection)
        expect(graph.edges[0]!.getDefinition().data.label).toBe('950 rows')
    })

    /** The label of each drawn edge by id, with all regions collapsed. */
    const labels = (workerProfiles: unknown[], nodes: unknown[], edges: { from_node: string, to_node: string }[]) => {
        const p = CircuitProfile.fromJson({
            metrics: [],
            worker_profiles: workerProfiles,
            graph: { nodes: { id: 'n', label: 'circuit', nodes }, edges }
        } as never).profile
        const selection = new CircuitSelection(new ExplicitSubSet(new Set(p.complexNodes.keys()), new Set()))
        return Object.fromEntries(Cytograph.fromProfile(p, selection).edges
            .map((e) => [e.getId(), e.getDefinition().data.label]))
    }
    const regionToSink = {
        nodes: [simple('n1', 'sink'), cluster('region', 'region', [simple('n2', 'map'), simple('n3', 'filter')])],
        edges: [{ from_node: 'n2', to_node: 'n1' }, { from_node: 'n3', to_node: 'n1' }]
    }

    it('sums the rows over all workers', () => {
        expect(labels(
            [{ metadata: { n2: outputRows(600) } }, { metadata: { n2: outputRows(400), n3: outputRows(0) } }],
            regionToSink.nodes, regionToSink.edges
        )).toEqual({ 'region->n1': '1.00k rows\n2 streams' })
    })

    it('shows no row count if one stream of the edge has no row count', () => {
        // n3 has no output statistics, so 600 rows would look like the sum over both streams.
        expect(labels([{ metadata: { n2: outputRows(600) } }], regionToSink.nodes, regionToSink.edges))
            .toEqual({ 'region->n1': '2 streams' })
    })

    it('shows a row count of 0', () => {
        expect(labels(
            [{ metadata: { n2: outputRows(0), n3: outputRows(0) } }],
            regionToSink.nodes, regionToSink.edges
        )).toEqual({ 'region->n1': '0 rows\n2 streams' })
    })

    it('shows the rows on a back edge', () => {
        // The profile merges the `Z^-1` pair into the output node, and the edge into it becomes a back edge.
        expect(labels(
            [{ metadata: { n1: outputRows(42) } }],
            [simple('n1', 'map'), simple('z', 'Z^-1'), simple('zo', 'Z^-1 (output)')],
            [{ from_node: 'zo', to_node: 'z' }, { from_node: 'n1', to_node: 'z' }, { from_node: 'zo', to_node: 'n1' }]
        )).toEqual({ 'n1->zo:back': '42 rows', 'zo->n1': undefined })
    })
})
