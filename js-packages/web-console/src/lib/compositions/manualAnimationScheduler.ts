import type { AnimationScheduler } from './progressBarAnimation'

/**
 * A scheduler for tests that the test controls manually. `advance` moves the
 * time forward. `paint` runs the callbacks that wait for a paint.
 */
export class ManualScheduler implements AnimationScheduler {
  private now = 0
  private timers: { at: number; callback: () => void }[] = []
  private paints: (() => void)[] = []

  after(ms: number, callback: () => void) {
    const timer = { at: this.now + ms, callback }
    this.timers.push(timer)
    return () => (this.timers = this.timers.filter((t) => t !== timer))
  }

  afterPaint(callback: () => void) {
    this.paints.push(callback)
    return () => (this.paints = this.paints.filter((p) => p !== callback))
  }

  paint() {
    const due = this.paints
    this.paints = []
    due.forEach((callback) => {
      callback()
    })
  }

  advance(ms: number) {
    this.now += ms
    for (;;) {
      const due = this.timers.find((t) => t.at <= this.now)
      if (!due) {
        return
      }
      this.timers = this.timers.filter((t) => t !== due)
      due.callback()
    }
  }

  get pendingCount() {
    return this.timers.length + this.paints.length
  }
}
