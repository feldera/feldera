import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  ProgressBarAnimator,
  type ProgressFrame,
  type ProgressSample
} from './progressBarAnimation'

// The file runs in the browser project for its `requestAnimationFrame`. The fake timers replace it.

const PERIOD = 2000
const HALF = PERIOD / 2

/** The data of the sample is the name of the activity, as in a label of a view. */
type Sample = ProgressSample<string>

const running = (key: number, combinedPercent: number, completedPercent = 0): Sample => ({
  key,
  active: true,
  combinedPercent,
  completedPercent,
  data: `running ${key}`
})

const idle = (key: number): Sample => ({
  key,
  active: false,
  combinedPercent: null,
  completedPercent: 0,
  data: `idle ${key}`
})

/** The part of a frame that a view renders. */
type View = {
  label: string
  combinedPercent: number | null
  completedPercent: number
  durationMs: number
  idle: boolean
}

const toView = (f: ProgressFrame<string>): View => ({
  label: f.sample.data,
  combinedPercent: f.combinedPercent,
  completedPercent: f.completedPercent,
  durationMs: f.durationMs,
  idle: !f.sample.active
})

/** Runs the callbacks that wait for a paint. The animator waits for two animation frames. */
const paint = () => {
  vi.advanceTimersToNextFrame()
  vi.advanceTimersToNextFrame()
}

const setup = () => {
  const frames: View[] = []
  const animator = new ProgressBarAnimator<string>(PERIOD, (frame) => frames.push(toView(frame)))
  const last = () => frames.at(-1)!
  return { frames, animator, last }
}

const frame = (
  label: string,
  combinedPercent: number | null,
  completedPercent: number,
  durationMs: number
): View => ({
  label,
  combinedPercent,
  completedPercent,
  durationMs,
  idle: label.startsWith('idle')
})

describe('ProgressBarAnimator', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('grows the bar of the first sample from 0', () => {
    const { animator, last } = setup()
    animator.update(running(1, 40, 20))
    expect(last()).toEqual(frame('running 1', 0, 0, 0))
    paint()
    expect(last()).toEqual(frame('running 1', 40, 20, HALF))
    vi.advanceTimersByTime(HALF)
    expect(last()).toEqual(frame('running 1', 40, 20, PERIOD))
    expect(vi.getTimerCount()).toBe(0)
  })

  it('shows an idle first sample as an empty, dimmed bar', () => {
    const { animator, last } = setup()
    animator.update(idle(1))
    expect(last()).toEqual(frame('idle 1', 0, 0, 0))
  })

  it('follows one activity over a full period per sample', () => {
    const { animator, last } = setup()
    animator.update(running(1, 10))
    paint()
    vi.advanceTimersByTime(HALF)
    animator.update(running(1, 50, 30))
    expect(last()).toEqual(frame('running 1', 50, 30, PERIOD))
    expect(vi.getTimerCount()).toBe(0)
  })

  it('grows the bar from 0 when an activity starts after an idle period', () => {
    const { animator, last } = setup()
    animator.update(idle(1))
    animator.update(running(2, 30))
    expect(last()).toEqual(frame('running 2', 0, 0, 0))
    paint()
    expect(last()).toEqual(frame('running 2', 30, 0, HALF))
  })

  it('shows a sample that arrived while starting once the start completes', () => {
    const { animator, last } = setup()
    animator.update(idle(1))
    animator.update(running(2, 30))
    animator.update(running(2, 50))
    paint()
    expect(last()).toEqual(frame('running 2', 50, 0, HALF))
  })

  it('finishes, snaps to 0 and grows again when the key changes', () => {
    const { animator, frames } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 40, 10))
    // Transaction 1 stays on the screen until its bar is full.
    expect(frames.at(-1)).toEqual(frame('running 1', 100, 100, HALF))

    vi.advanceTimersByTime(HALF)
    expect(frames.at(-1)).toEqual(frame('running 2', 0, 0, 0))

    paint()
    expect(frames.at(-1)).toEqual(frame('running 2', 40, 10, HALF))

    vi.advanceTimersByTime(HALF)
    expect(frames.at(-1)).toEqual(frame('running 2', 40, 10, PERIOD))
    expect(vi.getTimerCount()).toBe(0)
  })

  it('does not snap to 0 before the finish completes', () => {
    const { animator, last } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 40))
    vi.advanceTimersByTime(HALF - 1)
    expect(last()).toEqual(frame('running 1', 100, 100, HALF))
  })

  it('restarts once when the key skips several values between two samples', () => {
    const { animator, frames } = setup()
    animator.update(running(100, 60))
    animator.update(running(105, 40))
    vi.advanceTimersByTime(HALF)
    paint()
    vi.advanceTimersByTime(HALF)
    const finishes = frames.filter((f) => f.combinedPercent === 100)
    expect(finishes).toHaveLength(1)
    expect(new Set(frames.map((f) => f.label))).toEqual(new Set(['running 100', 'running 105']))
  })

  it('does not delay the finish when the key changes again while finishing', () => {
    const { animator, last } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    vi.advanceTimersByTime(HALF / 2)
    animator.update(running(3, 30))
    vi.advanceTimersByTime(HALF / 2)
    expect(last()).toEqual(frame('running 3', 0, 0, 0))
    paint()
    expect(last()).toEqual(frame('running 3', 30, 0, HALF))
  })

  it('finishes and then shows an idle bar when the activity ends', () => {
    const { animator, last } = setup()
    animator.update(running(1, 60))
    animator.update(idle(1))
    // The activity that stopped keeps its label until its bar is full.
    expect(last()).toEqual(frame('running 1', 100, 100, HALF))
    vi.advanceTimersByTime(HALF)
    expect(last()).toEqual(frame('idle 1', 0, 0, 0))
    expect(vi.getTimerCount()).toBe(0)
  })

  it('does not finish an idle bar when only the key changes', () => {
    const { animator, last } = setup()
    animator.update(idle(1))
    animator.update(idle(2))
    expect(last()).toEqual(frame('idle 2', 0, 0, 0))
    expect(vi.getTimerCount()).toBe(0)
  })

  it('restarts with the latest sample that arrived while finishing', () => {
    const { animator, last } = setup()
    animator.update(running(1, 60))
    animator.update(idle(1))
    animator.update(running(2, 25))
    // The move to 100% does not start again. The bar continues to the end.
    expect(last()).toEqual(frame('running 1', 100, 100, HALF))
    vi.advanceTimersByTime(HALF)
    paint()
    expect(last()).toEqual(frame('running 2', 25, 0, HALF))
  })

  it('shows a sample that arrived while restarting once the restart completes', () => {
    const { animator, last } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    vi.advanceTimersByTime(HALF)
    paint()
    animator.update(running(2, 70))
    expect(last()).toEqual(frame('running 2', 20, 0, HALF))
    vi.advanceTimersByTime(HALF)
    expect(last()).toEqual(frame('running 2', 70, 0, PERIOD))
  })

  it('finishes again when the key changes while restarting', () => {
    const { animator, last } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    vi.advanceTimersByTime(HALF)
    paint()
    animator.update(running(3, 10))
    // The bar of the activity on the screen moves to 100%, not the bar of the latest one.
    expect(last()).toEqual(frame('running 2', 100, 100, HALF))
    // The restart of key 2 is cancelled. The only pending timer ends the new move to 100%.
    expect(vi.getTimerCount()).toBe(1)
  })

  it('cancels pending steps on dispose', () => {
    const { animator, frames } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    animator.dispose()
    const count = frames.length
    vi.advanceTimersByTime(PERIOD)
    paint()
    expect(frames).toHaveLength(count)
  })
})
