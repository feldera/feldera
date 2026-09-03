// How big the map is drawn. The widget builds DOM in its constructor and this suite is headless, so what
// can be asked here is the arithmetic it is sized by - the same arithmetic a press on it is read
// through.

import { describe, expect, it } from 'vitest'
import { mapSize } from './navigator.js'

const area = (aspect: number): number => {
    const { width, height } = mapSize(aspect)
    return width * height
}

describe('mapSize', () => {
    it('gives a square circuit the baseline square', () => {
        expect(mapSize(1)).toEqual({ width: 100, height: 100 })
    })

    it('spends the same area on every shape', () => {
        // A wide circuit is drawn wide and a tall one tall, both at the weight on screen of a square
        // one.
        for (const aspect of [1, 1.5, 4, 1 / 3, 1 / 8]) {
            expect(area(aspect), `${aspect}`).toBeCloseTo(area(1), 5)
        }
    })

    it('draws the map in the shape of the circuit', () => {
        for (const aspect of [1, 2.5, 1 / 6, 40, 1 / 40]) {
            const { width, height } = mapSize(aspect)
            expect(width / height, `${aspect}`).toBeCloseTo(aspect, 5)
        }
    })

    it('holds every side to a maximum, shape before area', () => {
        // A circuit forty times taller than it is wide would be six hundred pixels tall at full area,
        // covering the pane the map floats on.
        for (const aspect of [40, 1 / 40]) {
            const { width, height } = mapSize(aspect)
            expect(Math.max(width, height), `${aspect}`).toBe(400)
            expect(area(aspect), `${aspect}`).toBeLessThan(area(1))
        }
    })
})
