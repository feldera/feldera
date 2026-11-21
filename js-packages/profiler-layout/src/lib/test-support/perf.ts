// Timing helpers for the performance suite (`*.perf.ts`, see `vitest.perf.config.ts`).

export interface PerfResult {
  name: string
  medianMs: number
  minMs: number
  maxMs: number
  runs: number
  samplesMs: number[]
}

/** What a run of the suite writes: the size of the circuits it measured, and the results. */
export interface PerfReport {
  circuits: Record<string, unknown>
  results: PerfResult[]
}

declare module 'vitest' {
  interface ProvidedContext {
    /** The `topLevelItems` option of `syntheticCircuit` for the diagram measurements. */
    perfTopLevelItems: number
  }
}

declare module 'vitest/browser' {
  interface BrowserCommands {
    /** Prints the results as a table. Writes the report as JSON to `$PROFILER_PERF_OUT` if that
     *  variable is set. */
    savePerfResults: (report: PerfReport) => Promise<void>
  }
}

/** Resolves after the browser paints the next frame. Cytoscape draws in an animation frame callback
 *  that it requested before the callback of this function, so the draw is done when the promise
 *  resolves. */
export const afterNextPaint = () =>
  new Promise<void>((resolve) => requestAnimationFrame(() => setTimeout(resolve, 0)))

/** Runs the garbage collector if Chromium exposes it (`--js-flags=--expose-gc`), so that the next
 *  measurement does not include a collection of the garbage from the previous run. */
const collectGarbage = () => (globalThis as { gc?: () => void }).gc?.()

export interface MeasureOptions {
  warmups: number
  runs: number
  /** Called before each run, not timed. */
  setup?: () => Promise<void> | void
  /** The timed action. It must resolve when the action is complete on screen. */
  run: () => Promise<void> | void
  /** Called after each run, not timed. */
  teardown?: () => Promise<void> | void
}

/** Times one action. A garbage collection and a paint come first, so the time includes neither of them. */
export async function timeAction(action: () => Promise<void> | void): Promise<number> {
  collectGarbage()
  await afterNextPaint()
  const start = performance.now()
  await action()
  return performance.now() - start
}

/** Times `run` `warmups + runs` times, and returns the statistics of the last `runs` samples. */
export async function measure(name: string, options: MeasureOptions): Promise<PerfResult> {
  const samplesMs: number[] = []
  for (let i = 0; i < options.warmups + options.runs; i++) {
    await options.setup?.()
    const elapsed = await timeAction(options.run)
    await options.teardown?.()
    if (i >= options.warmups) {
      samplesMs.push(elapsed)
    }
  }
  return summarize(name, samplesMs)
}

export interface MeasureFramesOptions {
  warmups: number
  runs: number
  /** Makes the diagram draw a new frame. */
  change: () => void
}

/** Times the frame that `change` causes, from the start of that frame until the browser paints it.
 *  The time does not include the wait for the frame to start, because the browser decides when a
 *  frame starts. */
export async function measureFrames(
  name: string,
  options: MeasureFramesOptions
): Promise<PerfResult> {
  const samplesMs: number[] = []
  for (let i = 0; i < options.warmups + options.runs; i++) {
    collectGarbage()
    await afterNextPaint()
    options.change()
    // All animation frame callbacks of one frame get the start time of that frame. Cytoscape draws
    // in a callback that it requested before this callback, so the time includes the draw.
    const elapsed = await new Promise<number>((resolve) =>
      requestAnimationFrame((frameStart) =>
        setTimeout(() => resolve(performance.now() - frameStart), 0)
      )
    )
    if (i >= options.warmups) {
      samplesMs.push(elapsed)
    }
  }
  return summarize(name, samplesMs)
}

export function summarize(name: string, samplesMs: number[]): PerfResult {
  const sorted = [...samplesMs].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  const medianMs = sorted.length % 2 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2
  return {
    name,
    medianMs,
    minMs: sorted[0]!,
    maxMs: sorted[sorted.length - 1]!,
    runs: samplesMs.length,
    samplesMs
  }
}
