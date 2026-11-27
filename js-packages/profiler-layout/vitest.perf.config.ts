import { writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { playwright } from '@vitest/browser-playwright'
import { defineConfig, mergeConfig } from 'vitest/config'
import type { BrowserCommand } from 'vitest/node'
import type { PerfReport } from './src/lib/test-support/perf.ts'
import viteConfig from './vite.config.ts'

// The performance suite (`bun run test-perf`). It is separate from `vitest.config.ts`, so that the
// normal test run and CI do not run it. It runs one file at a time in one real Chromium, with the
// garbage collector exposed so that `measure` can collect before each run.

const savePerfResults: BrowserCommand<[PerfReport]> = (_context, report) => {
  const rows = report.results.map((r) => [
    r.name,
    r.medianMs.toFixed(1),
    r.minMs.toFixed(1),
    r.maxMs.toFixed(1),
    String(r.runs)
  ])
  const header = ['measurement', 'median ms', 'min ms', 'max ms', 'runs']
  const widths = header.map((h, i) => Math.max(h.length, ...rows.map((row) => row[i]!.length)))
  const line = (cells: string[]) =>
    cells
      .map((cell, i) => (i === 0 ? cell.padEnd(widths[i]!) : cell.padStart(widths[i]!)))
      .join('  ')
  const circuits = Object.entries(report.circuits).map(
    ([use, size]) => `${use}: ${JSON.stringify(size)}`
  )
  process.stdout.write(`\n${[...circuits, '', line(header), ...rows.map(line)].join('\n')}\n\n`)

  const outPath = process.env.PROFILER_PERF_OUT
  if (outPath) {
    writeFileSync(resolve(outPath), `${JSON.stringify(report, null, 2)}\n`)
    process.stdout.write(`Wrote ${resolve(outPath)}\n`)
  }
}

export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      name: 'perf',
      include: ['src/**/*.perf.ts'],
      setupFiles: ['./test/setup.ts'],
      testTimeout: 120_000,
      provide: { perfTopLevelItems: Number(process.env.PROFILER_PERF_TOP_LEVEL_ITEMS ?? 60) },
      fileParallelism: false,
      browser: {
        enabled: true,
        provider: playwright({ launchOptions: { args: ['--js-flags=--expose-gc'] } }),
        headless: true,
        viewport: { width: 1280, height: 900 },
        instances: [{ browser: 'chromium' as const }],
        commands: { savePerfResults }
      }
    }
  })
)
