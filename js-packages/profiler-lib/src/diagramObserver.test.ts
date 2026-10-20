// That one observer cannot take the others down with it.

import { describe, expect, it, vi } from 'vitest'

import { notifyObservers, type DiagramObserver } from './diagramObserver.js'

describe('notifying the observers', () => {
    it('reaches them in the order they are given', () => {
        const told: string[] = []
        const observer = (name: string): DiagramObserver => ({ layoutSettled: () => told.push(name) })
        notifyObservers(
            [observer('view'), observer('picture')],
            (o) => o.layoutSettled?.()
        )
        expect(told).toEqual(['view', 'picture'])
    })

    it('carries on past one that throws, and says so', () => {
        // The picture held over the layout comes down in the last observer's `layoutSettled`. If a
        // throw in the first one skipped it, the diagram would stay under an image of itself for good.
        const reported = vi.spyOn(console, 'error').mockImplementation(() => { })
        const told: string[] = []
        notifyObservers(
            [
                { layoutSettled: () => { throw new Error('no node to center on') } },
                { layoutSettled: () => told.push('picture') }
            ],
            (o) => o.layoutSettled?.()
        )
        expect(told).toEqual(['picture'])
        expect(reported).toHaveBeenCalledOnce()
        reported.mockRestore()
    })

    it('skips an observer with no opinion on the hook', () => {
        expect(() => notifyObservers([{}], (o) => o.layoutSettled?.())).not.toThrow()
    })
})
