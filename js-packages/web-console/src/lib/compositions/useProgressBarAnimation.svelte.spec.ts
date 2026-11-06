import { flushSync } from 'svelte'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ProgressFrame, ProgressSample } from './progressBarAnimation'
import { useProgressBarAnimation } from './useProgressBarAnimation.svelte'

const PERIOD = 2000
const HALF = PERIOD / 2

const running = (key: number, combinedPercent: number): ProgressSample<string> => ({
  key,
  active: true,
  combinedPercent,
  completedPercent: 0,
  data: `transaction ${key}`
})

/** Runs the callbacks that wait for a paint. The animator waits for two animation frames. */
const paint = () => {
  vi.advanceTimersToNextFrame()
  vi.advanceTimersToNextFrame()
}

/** Runs the composition on a reactive sample, as a component does. */
const setup = (initial: ProgressSample<string>) => {
  const polled = $state({ sample: initial })
  let bar: { readonly current: ProgressFrame<string> } = undefined!
  const stop = $effect.root(() => {
    bar = useProgressBarAnimation(() => polled.sample, PERIOD)
  })
  const poll = (sample: ProgressSample<string>) => {
    polled.sample = sample
    flushSync()
  }
  /** The label and the bar width that the screen shows. */
  const view = () => [bar.current.sample.data, bar.current.combinedPercent]
  return { poll, stop, view }
}

describe('useProgressBarAnimation', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('shows an empty bar for the first sample before any effect runs', () => {
    const { stop, view } = setup(running(1, 40))
    expect(view()).toEqual(['transaction 1', 0])
    stop()
  })

  it('grows the bar of the first sample from 0', () => {
    const { stop, view } = setup(running(1, 40))
    flushSync()
    expect(view()).toEqual(['transaction 1', 0])
    paint()
    expect(view()).toEqual(['transaction 1', 40])
    stop()
  })

  it('follows a new sample of the same activity', () => {
    const { poll, stop, view } = setup(running(1, 40))
    flushSync()
    paint()
    vi.advanceTimersByTime(HALF)
    poll(running(1, 70))
    expect(view()).toEqual(['transaction 1', 70])
    stop()
  })

  it('keeps the old activity on screen until its bar is full', () => {
    const { poll, stop, view } = setup(running(1, 40))
    flushSync()
    poll(running(2, 30))
    expect(view()).toEqual(['transaction 1', 100])
    vi.advanceTimersByTime(HALF)
    expect(view()).toEqual(['transaction 2', 0])
    paint()
    expect(view()).toEqual(['transaction 2', 30])
    stop()
  })

  it('cancels pending steps when its owner is destroyed', () => {
    const { poll, stop } = setup(running(1, 40))
    flushSync()
    poll(running(2, 30))
    expect(vi.getTimerCount()).toBe(1)
    stop()
    expect(vi.getTimerCount()).toBe(0)
  })
})
