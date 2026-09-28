import { flushSync } from 'svelte'
import { describe, expect, it } from 'vitest'
import { ManualScheduler } from './manualAnimationScheduler'
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

/** Runs the composition on a reactive sample, as a component does. */
const setup = (initial: ProgressSample<string>) => {
  const polled = $state({ sample: initial })
  const scheduler = new ManualScheduler()
  let bar: { readonly current: ProgressFrame<string> } = undefined!
  const stop = $effect.root(() => {
    bar = useProgressBarAnimation(() => polled.sample, PERIOD, scheduler)
  })
  const poll = (sample: ProgressSample<string>) => {
    polled.sample = sample
    flushSync()
  }
  /** The label and the bar width that the screen shows. */
  const view = () => [bar.current.sample.data, bar.current.combinedPercent]
  return { poll, scheduler, stop, view }
}

describe('useProgressBarAnimation', () => {
  it('shows the first sample before any effect runs', () => {
    const { stop, view } = setup(running(1, 40))
    expect(view()).toEqual(['transaction 1', 40])
    stop()
  })

  it('follows a new sample of the same activity', () => {
    const { poll, stop, view } = setup(running(1, 40))
    flushSync()
    poll(running(1, 70))
    expect(view()).toEqual(['transaction 1', 70])
    stop()
  })

  it('keeps the old activity on screen until its bar is full', () => {
    const { poll, scheduler, stop, view } = setup(running(1, 40))
    flushSync()
    poll(running(2, 30))
    expect(view()).toEqual(['transaction 1', 100])
    scheduler.advance(HALF)
    expect(view()).toEqual(['transaction 2', 0])
    scheduler.paint()
    expect(view()).toEqual(['transaction 2', 30])
    stop()
  })

  it('cancels pending steps when its owner is destroyed', () => {
    const { poll, scheduler, stop } = setup(running(1, 40))
    flushSync()
    poll(running(2, 30))
    expect(scheduler.pendingCount).toBe(1)
    stop()
    expect(scheduler.pendingCount).toBe(0)
  })
})
