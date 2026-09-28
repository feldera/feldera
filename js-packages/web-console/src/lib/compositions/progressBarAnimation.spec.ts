import { describe, expect, it } from 'vitest'
import { ManualScheduler } from './manualAnimationScheduler'
import {
  ProgressBarAnimator,
  type ProgressFrame,
  type ProgressSample
} from './progressBarAnimation'

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

const setup = () => {
  const frames: View[] = []
  const scheduler = new ManualScheduler()
  const animator = new ProgressBarAnimator<string>(
    PERIOD,
    (frame) => frames.push(toView(frame)),
    scheduler
  )
  const last = () => frames.at(-1)!
  return { frames, scheduler, animator, last }
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
  it('shows the first sample', () => {
    const { animator, last } = setup()
    animator.update(running(1, 40, 20))
    expect(last()).toEqual(frame('running 1', 40, 20, PERIOD))
  })

  it('shows an idle first sample as an empty, dimmed bar', () => {
    const { animator, last } = setup()
    animator.update(idle(1))
    expect(last()).toEqual(frame('idle 1', 0, 0, 0))
  })

  it('follows one activity over a full period per sample', () => {
    const { animator, last, scheduler } = setup()
    animator.update(running(1, 10))
    animator.update(running(1, 50, 30))
    expect(last()).toEqual(frame('running 1', 50, 30, PERIOD))
    expect(scheduler.pendingCount).toBe(0)
  })

  it('grows from an empty bar when an activity starts after an idle period', () => {
    const { animator, last, scheduler } = setup()
    animator.update(idle(1))
    animator.update(running(2, 30))
    expect(last()).toEqual(frame('running 2', 30, 0, PERIOD))
    expect(scheduler.pendingCount).toBe(0)
  })

  it('finishes, snaps to 0 and grows again when the key changes', () => {
    const { animator, frames, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 40, 10))
    // Transaction 1 stays on the screen until its bar is full.
    expect(frames.at(-1)).toEqual(frame('running 1', 100, 100, HALF))

    scheduler.advance(HALF)
    expect(frames.at(-1)).toEqual(frame('running 2', 0, 0, 0))

    scheduler.paint()
    expect(frames.at(-1)).toEqual(frame('running 2', 40, 10, HALF))

    scheduler.advance(HALF)
    expect(frames.at(-1)).toEqual(frame('running 2', 40, 10, PERIOD))
    expect(scheduler.pendingCount).toBe(0)
  })

  it('does not snap to 0 before the finish completes', () => {
    const { animator, last, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 40))
    scheduler.advance(HALF - 1)
    expect(last()).toEqual(frame('running 1', 100, 100, HALF))
  })

  it('restarts once when the key skips several values between two samples', () => {
    const { animator, frames, scheduler } = setup()
    animator.update(running(100, 60))
    animator.update(running(105, 40))
    scheduler.advance(HALF)
    scheduler.paint()
    scheduler.advance(HALF)
    const finishes = frames.filter((f) => f.combinedPercent === 100)
    expect(finishes).toHaveLength(1)
    // The screen never shows the activities that the poll skipped.
    expect(new Set(frames.map((f) => f.label))).toEqual(new Set(['running 100', 'running 105']))
  })

  it('does not delay the finish when the key changes again while finishing', () => {
    const { animator, last, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    scheduler.advance(HALF / 2)
    animator.update(running(3, 30))
    scheduler.advance(HALF / 2)
    expect(last()).toEqual(frame('running 3', 0, 0, 0))
    scheduler.paint()
    expect(last()).toEqual(frame('running 3', 30, 0, HALF))
  })

  it('finishes and then shows an idle bar when the activity ends', () => {
    const { animator, last, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(idle(1))
    // The activity that stopped keeps its label until its bar is full.
    expect(last()).toEqual(frame('running 1', 100, 100, HALF))
    scheduler.advance(HALF)
    expect(last()).toEqual(frame('idle 1', 0, 0, 0))
    expect(scheduler.pendingCount).toBe(0)
  })

  it('does not finish an idle bar when only the key changes', () => {
    const { animator, last, scheduler } = setup()
    animator.update(idle(1))
    animator.update(idle(2))
    expect(last()).toEqual(frame('idle 2', 0, 0, 0))
    expect(scheduler.pendingCount).toBe(0)
  })

  it('restarts with the latest sample that arrived while finishing', () => {
    const { animator, last, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(idle(1))
    animator.update(running(2, 25))
    // The move to 100% does not start again. The bar continues to the end.
    expect(last()).toEqual(frame('running 1', 100, 100, HALF))
    scheduler.advance(HALF)
    scheduler.paint()
    expect(last()).toEqual(frame('running 2', 25, 0, HALF))
  })

  it('shows a sample that arrived while restarting once the restart completes', () => {
    const { animator, last, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    scheduler.advance(HALF)
    scheduler.paint()
    animator.update(running(2, 70))
    expect(last()).toEqual(frame('running 2', 20, 0, HALF))
    scheduler.advance(HALF)
    expect(last()).toEqual(frame('running 2', 70, 0, PERIOD))
  })

  it('finishes again when the key changes while restarting', () => {
    const { animator, last, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    scheduler.advance(HALF)
    scheduler.paint()
    animator.update(running(3, 10))
    // The bar of the activity on the screen moves to 100%, not the bar of the latest one.
    expect(last()).toEqual(frame('running 2', 100, 100, HALF))
    // The restart of key 2 is cancelled. Thus, only the new move to 100% waits.
    expect(scheduler.pendingCount).toBe(1)
  })

  it('cancels pending steps on dispose', () => {
    const { animator, frames, scheduler } = setup()
    animator.update(running(1, 60))
    animator.update(running(2, 20))
    animator.dispose()
    const count = frames.length
    scheduler.advance(PERIOD)
    scheduler.paint()
    expect(frames).toHaveLength(count)
  })
})
