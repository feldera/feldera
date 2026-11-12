// Unit tests for the layout cache: the key changes when the input of ELK changes, and the cache keeps
// only the most recently used entries. The browser tests in profiler-layout check that a cache hit
// gives the positions of the ELK layout.

import cytoscape, { type ElementDefinition } from 'cytoscape'
import { describe, expect, it } from 'vitest'
import { type ElkLayoutOptions, LayoutCache, layoutCacheKey, leafPositions } from './layoutCache.js'

const OPTIONS: ElkLayoutOptions = {
    name: 'elk',
    nodeDimensionsIncludeLabels: true,
    nodeLayoutOptions: (node) => (node.isParent() ? { 'elk.nodeSize.minimum': `(0,${node.data('min_width')})` } : {}),
    elk: { algorithm: 'layered', 'elk.hierarchyHandling': 'INCLUDE_CHILDREN' }
}

/** A region with two operators, an operator outside it, and two edges. */
const elements = (): ElementDefinition[] => [
    { data: { id: 'region', min_width: 100 } },
    { data: { id: 'a', parent: 'region', width: 80 } },
    { data: { id: 'b', parent: 'region', width: 80 } },
    { data: { id: 'c', width: 80 } },
    { data: { id: 'a->b', source: 'a', target: 'b' } },
    { data: { id: 'b->c', source: 'b', target: 'c' } }
]

const keyOf = (elements: ElementDefinition[], options = OPTIONS) => {
    const cy = cytoscape({
        headless: true,
        styleEnabled: true,
        style: [{ selector: 'node', style: { width: 'data(width)', height: 30 } }],
        elements
    })
    return layoutCacheKey(cy, options)
}

/** `elements()` with `change` applied to the element with id `id`. */
const changed = (id: string, change: Record<string, unknown>) =>
    elements().map((element) => (element.data.id === id ? { data: { ...element.data, ...change } } : element))

describe('layoutCacheKey', () => {
    it('is the same for the same graph', () => {
        expect(keyOf(elements())).toBe(keyOf(elements()))
    })

    it('changes when a part of the ELK input changes', () => {
        const key = keyOf(elements())
        const variants: Record<string, ElementDefinition[]> = {
            'node id': changed('c', { id: 'd' }).map((element) =>
                element.data.target === 'c' ? { data: { ...element.data, target: 'd' } } : element
            ),
            'parent': changed('b', { parent: undefined }),
            'node size': changed('c', { width: 81 }),
            'node options': changed('region', { min_width: 101 }),
            'edge id': changed('b->c', { id: 'b->c:back' }),
            'edge target': changed('a->b', { target: 'c' }),
            'edge order': [...elements().slice(0, 4), elements()[5]!, elements()[4]!],
            'node order': [elements()[0]!, elements()[2]!, elements()[1]!, ...elements().slice(3)]
        }
        for (const [part, variant] of Object.entries(variants)) {
            expect(keyOf(variant), part).not.toBe(key)
        }
        const elkOptions = { ...OPTIONS, elk: { ...OPTIONS.elk, 'elk.layered.thoroughness': 2 } }
        expect(keyOf(elements(), elkOptions), 'ELK options').not.toBe(key)
    })
})

describe('LayoutCache', () => {
    it('keeps the most recently used entries', () => {
        const cache = new LayoutCache(2)
        const positions = (x: number) => ({ a: { x, y: 0 } })
        cache.set('one', positions(1))
        cache.set('two', positions(2))
        // Reading `one` makes `two` the oldest entry.
        expect(cache.get('one')).toEqual(positions(1))
        cache.set('three', positions(3))
        expect(cache.get('two')).toBeUndefined()
        expect(cache.get('one')).toEqual(positions(1))
        expect(cache.get('three')).toEqual(positions(3))
    })
})

describe('leafPositions', () => {
    it('copies the positions of the nodes that are not parents', () => {
        const cy = cytoscape({ headless: true, elements: elements() })
        cy.$id('c').position({ x: 5, y: 6 })
        const positions = leafPositions(cy)
        cy.$id('c').position({ x: 7, y: 8 })
        expect(Object.keys(positions).sort()).toEqual(['a', 'b', 'c'])
        expect(positions.c).toEqual({ x: 5, y: 6 })
    })
})
