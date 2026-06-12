import { describe, expect, it } from 'vitest'
import type { ProfilerCallbacks } from './profiler.js'

// `onRenderingChange` is the lifecycle hook the profiler UI consumes to drive its
// "rendering" progress bar (issue: in-progress feedback during the ELK layout pass,
// which dominates first-render wall time on large profiles). These tests pin the
// contract — the callback is optional, is invoked with a boolean, and pairs `true`
// (layout starts) with `false` (layout finishes).

describe('ProfilerCallbacks.onRenderingChange contract', () => {
    it('is optional — consumers may omit it without satisfying the type', () => {
        // No `onRenderingChange` field — must still satisfy the interface.
        const callbacks: ProfilerCallbacks = {
            displayNodeAttributes: () => {},
            displayTopNodes: () => {},
            onMetricsChanged: () => {},
            displayMessage: () => {},
            onError: () => {}
        }
        // Force a runtime use so TS doesn't elide the assignment in --noEmit checks.
        expect(typeof callbacks.displayMessage).toBe('function')
    })

    it('accepts a (rendering: boolean) => void implementation', () => {
        const calls: boolean[] = []
        const callbacks: ProfilerCallbacks = {
            displayNodeAttributes: () => {},
            displayTopNodes: () => {},
            onMetricsChanged: () => {},
            displayMessage: () => {},
            onError: () => {},
            onRenderingChange: (rendering) => calls.push(rendering)
        }
        callbacks.onRenderingChange?.(true)
        callbacks.onRenderingChange?.(false)
        // Pair semantics: a `true` is always followed by a `false`.
        expect(calls).toEqual([true, false])
    })
})
