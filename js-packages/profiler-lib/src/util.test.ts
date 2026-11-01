import { describe, expect, it } from 'vitest'
import { Graph } from './util.js'

describe('Graph.addEdge', () => {
    it('gives each edge between the same two nodes its own id', () => {
        const graph = new Graph<string>()
        for (const node of ['a', 'b', 'c']) graph.addNode(node)
        graph.addEdge('a', 'b', 1, false)
        graph.addEdge('b', 'a', 1, false)
        graph.addEdge('a', 'b', 1, false)
        graph.addEdge('a', 'c', 1, false)
        graph.addEdge('a', 'b', 1, true)
        expect([...graph.edges].map((e) => e.id)).toEqual(['a->b', 'b->a', 'a->b:2', 'a->c', 'a->b:3'])
    })
})
