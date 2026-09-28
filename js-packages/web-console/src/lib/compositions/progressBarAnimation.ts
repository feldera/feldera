/**
 * Controls the animation of a progress bar that gets its data from a poll.
 *
 * Rules:
 * - While one activity (for example, a transaction) runs, the bar moves to
 *   each new sample in one poll period.
 * - When the activity stops, or a different activity replaces it, the bar
 *   moves to 100% in half a period.
 * - Then, if a new activity runs, the bar goes to 0 immediately. It moves to
 *   the new sample in the other half of the period.
 * - If many activities stop in one period (for example, transactions 100 to
 *   105), the bar goes to 100% and starts again one time only.
 *
 * Each frame contains the sample that it shows. Thus, if a view shows only
 * frames, its labels (for example, the transaction ID) change at the same
 * time as the bar. The old activity stays on the screen until its bar is full.
 */

/** One sample of the activity, from the poll. */
export type ProgressSample<T> = {
  /** Identifies the activity. A different key shows that a different activity started. */
  key: unknown
  /** Is `false` when no activity runs. */
  active: boolean
  /** The completed work plus the partly processed work, in percent. Is `null` if not known. */
  combinedPercent: number | null
  /** The completed work, in percent. */
  completedPercent: number
  /** The data that the view shows near the bar, for example a status and operator counts. */
  data: T
}

/** The data that the view shows, and the time that the bar uses to move to its new width. */
export type ProgressFrame<T> = {
  /**
   * The sample of the bar. While the bar moves to 100%, this sample is older
   * than the latest sample.
   */
  sample: ProgressSample<T>
  combinedPercent: number | null
  completedPercent: number
  durationMs: number
}

export type AnimationScheduler = {
  /** Runs `callback` after `ms`. Returns a function that cancels the callback. */
  after(ms: number, callback: () => void): () => void
  /**
   * Runs `callback` after the browser paints the current frame. Thus, the
   * browser does not merge a style set before the paint with a style set in
   * `callback`.
   */
  afterPaint(callback: () => void): () => void
}

export const browserScheduler: AnimationScheduler = {
  after(ms, callback) {
    const id = setTimeout(callback, ms)
    return () => clearTimeout(id)
  },
  afterPaint(callback) {
    // The browser paints the pending style after the first frame.
    // The second frame runs the callback.
    let id = requestAnimationFrame(() => {
      id = requestAnimationFrame(callback)
    })
    return () => cancelAnimationFrame(id)
  }
}

/** Makes the frame that shows `sample` when no activity stopped. */
export const followFrame = <T>(sample: ProgressSample<T>, periodMs: number): ProgressFrame<T> =>
  sample.active
    ? {
        sample,
        combinedPercent: sample.combinedPercent,
        completedPercent: sample.completedPercent,
        durationMs: periodMs
      }
    : // An idle bar shows no progress. It goes to 0 immediately, not slowly.
      { sample, combinedPercent: 0, completedPercent: 0, durationMs: 0 }

type Phase = 'follow' | 'finish' | 'restart'

export class ProgressBarAnimator<T> {
  private phase: Phase = 'follow'
  private latest: ProgressSample<T> | undefined
  /** The sample of the last frame that was rendered. */
  private shown: ProgressSample<T> | undefined
  private cancelPending: () => void = () => {}

  constructor(
    private readonly periodMs: number,
    private readonly render: (frame: ProgressFrame<T>) => void,
    private readonly scheduler: AnimationScheduler = browserScheduler
  ) {}

  /** Receives a new sample from the poll. */
  update(sample: ProgressSample<T>) {
    const previous = this.latest
    this.latest = sample
    if (!previous) {
      this.show(followFrame(sample, this.periodMs))
      return
    }
    const hasEnded = previous.active && (!sample.active || sample.key !== previous.key)
    if (hasEnded && this.phase !== 'finish') {
      this.finish()
      return
    }
    if (this.phase === 'follow') {
      this.show(followFrame(sample, this.periodMs))
    }
    // Else, the sequence that runs now shows the latest sample at its end.
  }

  /** Cancels the steps that did not run. Call it when the bar is removed. */
  dispose() {
    this.cancelPending()
  }

  private get halfPeriodMs() {
    return this.periodMs / 2
  }

  private show(frame: ProgressFrame<T>) {
    this.shown = frame.sample
    this.render(frame)
  }

  private finish() {
    this.cancelPending()
    this.phase = 'finish'
    // The activity that stopped stays on the screen until its bar is full.
    this.show({
      sample: this.shown!,
      combinedPercent: 100,
      completedPercent: 100,
      durationMs: this.halfPeriodMs
    })
    this.cancelPending = this.scheduler.after(this.halfPeriodMs, () => this.restart())
  }

  private restart() {
    const next = this.latest!
    if (!next.active) {
      this.phase = 'follow'
      this.show(followFrame(next, this.periodMs))
      return
    }
    this.phase = 'restart'
    this.show({ sample: next, combinedPercent: 0, completedPercent: 0, durationMs: 0 })
    this.cancelPending = this.scheduler.afterPaint(() => {
      this.show({ ...followFrame(this.latest!, this.periodMs), durationMs: this.halfPeriodMs })
      this.cancelPending = this.scheduler.after(this.halfPeriodMs, () => {
        this.phase = 'follow'
        this.show(followFrame(this.latest!, this.periodMs))
      })
    })
  }
}
