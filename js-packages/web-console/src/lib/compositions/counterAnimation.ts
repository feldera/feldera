import { type AnimationScheduler, browserScheduler } from './progressBarAnimation'

/**
 * Controls the animation of a counter that gets its value from a poll, for
 * example the ID of the current transaction.
 *
 * Rules:
 * - While the activity runs in two polls in sequence, the counter counts up to
 *   the new value in one poll period. Thus, it counts at the rate that the
 *   value grew between the polls.
 * - In all other cases, the counter jumps to the new value. Thus, the first
 *   value that the view shows for an activity is correct.
 * - The counter also jumps when the new value is not a plausible count, for
 *   example when it decreases or is not an integer. Thus, a value that stops
 *   being a counter (for example, a hash) shows correctly.
 */

/** A larger growth in one poll is not a count. */
export const MAX_COUNTED_STEP = 1_000_000
/** The minimum time between two changes of the shown value. */
const MIN_TICK_MS = 50

/** One sample of the counter, from the poll. */
export type CounterSample = {
  value: number
  /** Is `false` when no activity runs. */
  active: boolean
}

export type CounterOptions = {
  scheduler?: AnimationScheduler
}

export class CounterAnimator {
  private previous: CounterSample | undefined
  /** The value of the last render. */
  private shown: number | undefined
  private cancelTick: () => void = () => {}
  private readonly scheduler: AnimationScheduler

  constructor(
    private readonly periodMs: number,
    private readonly render: (value: number) => void,
    options: CounterOptions = {}
  ) {
    this.scheduler = options.scheduler ?? browserScheduler
  }

  /** Receives a new sample from the poll. */
  update(sample: CounterSample) {
    const previous = this.previous
    this.previous = sample
    if (sample.value === previous?.value) {
      // A count to this value can run now. It continues.
      return
    }
    this.cancelTick()
    const from = this.shown
    const isRunning = sample.active && !!previous?.active
    if (from === undefined || !isRunning || !isCountStep(sample.value - from)) {
      this.show(sample.value)
      return
    }
    this.countUp(from, sample.value)
  }

  /** Cancels the steps that did not run. Call it when the counter is removed. */
  dispose() {
    this.cancelTick()
  }

  private show(value: number) {
    this.shown = value
    this.render(value)
  }

  /** Counts from `from` to `to` in one poll period. */
  private countUp(from: number, to: number) {
    const startMs = this.scheduler.now()
    const tickMs = Math.max(MIN_TICK_MS, this.periodMs / (to - from))
    const tick = () => {
      const fraction = Math.min(1, (this.scheduler.now() - startMs) / this.periodMs)
      this.show(from + Math.round((to - from) * fraction))
      if (fraction < 1) {
        this.cancelTick = this.scheduler.after(tickMs, tick)
      }
    }
    this.cancelTick = this.scheduler.after(tickMs, tick)
  }
}

const isCountStep = (step: number) =>
  Number.isSafeInteger(step) && step > 0 && step <= MAX_COUNTED_STEP
