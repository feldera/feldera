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
    /** The size of the synthetic circuit, see `syntheticCircuit`. */
    perfTopLevelItems: number
  }
}

declare module 'vitest/browser' {
  interface BrowserCommands {
    /** Print the results as a table, and write the report as JSON to `$PROFILER_PERF_OUT` when it
     *  is set. */
    savePerfResults: (report: PerfReport) => Promise<void>
  }
}

/** Resolve after the browser paints the next frame. Cytoscape draws in an animation frame callback
 *  that it requested before this one, so the draw is done when this resolves. */
export const afterNextPaint = () =>
  new Promise<void>((resolve) => requestAnimationFrame(() => setTimeout(resolve, 0)))

/** Run the garbage collector when Chromium exposes it (`--js-flags=--expose-gc`), so that a
 *  collection left over from the previous run does not land in the next measurement. */
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

/** Time one action, after a garbage collection and a paint, so that neither lands in the time. */
export async function timeAction(action: () => Promise<void> | void): Promise<number> {
  collectGarbage()
  await afterNextPaint()
  const start = performance.now()
  await action()
  return performance.now() - start
}

/** Time `run` `warmups + runs` times, and give the statistics of the last `runs`. */
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
