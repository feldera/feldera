/**
 * Controls the animation of a progress bar. The view polls the pipeline for
 * the progress of an activity, for example a transaction commit, and passes
 * each result to `ProgressBarAnimator` as a sample.
 *
 * Rules:
 * - While one activity runs, the bar moves to the percent of each new sample
 *   in one poll period.
 * - When the activity stops, or a different activity replaces it, the bar
 *   moves to 100% in half a period.
 * - Then, if a new activity runs, the bar jumps to 0. It moves to the percent
 *   of the new sample in the other half of the period.
 * - When an activity starts after a poll with no activity, the bar also
 *   starts from 0.
 * Thus, the bar of each activity starts at 0 and ends at 100%.
 *
 * The animator does not change the view directly. For each step, it calls
 * `render` with a `ProgressFrame`. A frame holds the widths to show, the time
 * to move to them, and the sample that these widths belong to. The view takes
 * all the labels near the bar, for example the status chip, from the sample of
 * the frame. Thus, the labels change at the same time as the bar, and the
 * labels of an activity that stopped stay on the screen until its bar is full.
 */

/** The progress of the activity in one poll. */
export type ProgressSample<T> = {
  /** Identifies the activity. A different key shows that a different activity started. */
  key: unknown
  /** Is `false` when no activity runs. */
  active: boolean
  /**
   * The completed work plus the partly processed work, from 0 to 100. Is
   * `null` if not known.
   */
  combinedPercent: number | null
  /** The completed work, from 0 to 100. */
  completedPercent: number
  /** The data that the view shows near the bar, for example a status and operator counts. */
  data: T
}

/** One step of the animation. The view renders the bar and its labels from this frame. */
export type ProgressFrame<T> = {
  /**
   * The sample that the view takes its labels from. Usually it is the latest
   * sample. While the bar of an activity that stopped moves to 100%, it is
   * the last sample of that activity.
   */
  sample: ProgressSample<T>
  /** The widths to show, from 0 to 100. They can differ from the percents of `sample`. */
  combinedPercent: number | null
  completedPercent: number
  /** The time to move from the current widths to the new widths. */
  durationMs: number
}

/** Makes the frame that shows an empty bar for `sample` immediately. */
export const zeroFrame = <T>(sample: ProgressSample<T>): ProgressFrame<T> => ({
  sample,
  combinedPercent: 0,
  completedPercent: 0,
  durationMs: 0
})

/** Makes the frame that moves the bar to the percents of `sample` in `periodMs`. */
export const followFrame = <T>(sample: ProgressSample<T>, periodMs: number): ProgressFrame<T> =>
  sample.active
    ? {
        sample,
        combinedPercent: sample.combinedPercent,
        completedPercent: sample.completedPercent,
        durationMs: periodMs
      }
    : // An idle bar shows no progress. It jumps to 0.
      zeroFrame(sample)

/** Runs `callback` after `ms`. Returns a function that cancels the callback. */
const after = (ms: number, callback: () => void) => {
  const id = setTimeout(callback, ms)
  return () => clearTimeout(id)
}

/**
 * Runs `callback` after the browser paints the styles that are set now. Thus,
 * the browser shows a width of 0 before `callback` sets a new width, and the
 * CSS transition starts from 0. Returns a function that cancels the callback.
 */
const afterPaint = (callback: () => void) => {
  // A `requestAnimationFrame` callback runs before the paint. Thus, the
  // callback runs in the second animation frame, after the first paint.
  let id = requestAnimationFrame(() => {
    id = requestAnimationFrame(callback)
  })
  return () => cancelAnimationFrame(id)
}

/**
 * The step of the animation that runs now:
 * - `follow`: the bar moves to the percent of each new sample.
 * - `finish`: the bar of an activity that stopped moves to 100%.
 * - `restart`: the bar grows from 0 to the percent of the latest sample.
 */
type Phase = 'follow' | 'finish' | 'restart'

export class ProgressBarAnimator<T> {
  private phase: Phase = 'follow'
  private latest: ProgressSample<T> | undefined
  /** The sample of the last frame that was rendered. */
  private shown: ProgressSample<T> | undefined
  private cancelPending: () => void = () => {}

  constructor(
    private readonly periodMs: number,
    private readonly render: (frame: ProgressFrame<T>) => void
  ) {}

  /** Receives a new sample from the poll. */
  update(sample: ProgressSample<T>) {
    const previous = this.latest
    this.latest = sample
    const hasStarted = sample.active && !previous?.active
    const hasEnded = !!previous?.active && (!sample.active || sample.key !== previous.key)
    if (hasEnded && this.phase !== 'finish') {
      this.finish()
      return
    }
    if (hasStarted && this.phase === 'follow') {
      this.restart(this.periodMs / 2)
      return
    }
    if (this.phase === 'follow') {
      this.show(followFrame(sample, this.periodMs))
    }
    // In the `finish` and `restart` phases, the bar shows the latest sample when the phase ends.
  }

  /** Cancels the steps that did not run yet. Call it when the view removes the bar. */
  dispose() {
    this.cancelPending()
  }

  private show(frame: ProgressFrame<T>) {
    this.shown = frame.sample
    this.render(frame)
  }

  /** Moves the bar of the shown activity to 100%. Then it restarts the bar for the latest sample. */
  private finish() {
    this.cancelPending()
    this.phase = 'finish'
    const stepMs = this.periodMs / 2
    // The activity that stopped stays on the screen until its bar is full.
    this.show({
      sample: this.shown!,
      combinedPercent: 100,
      completedPercent: 100,
      durationMs: stepMs
    })
    this.cancelPending = after(stepMs, () => this.restart(stepMs))
  }

  /**
   * Grows the bar from 0 to the percent of the latest sample in `stepMs`. If
   * the latest sample is idle, shows an empty bar.
   */
  private restart(stepMs: number) {
    if (!this.latest!.active) {
      this.phase = 'follow'
      this.show(followFrame(this.latest!, this.periodMs))
      return
    }
    this.phase = 'restart'
    this.fromZero(this.latest!, () => {
      this.show({ ...followFrame(this.latest!, this.periodMs), durationMs: stepMs })
      this.cancelPending = after(stepMs, () => {
        this.phase = 'follow'
        this.show(followFrame(this.latest!, this.periodMs))
      })
    })
  }

  /** Sets the bar of `sample` to 0 immediately, and calls `grow` after the paint. */
  private fromZero(sample: ProgressSample<T>, grow: () => void) {
    this.show(zeroFrame(sample))
    this.cancelPending = afterPaint(grow)
  }
}
