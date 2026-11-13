// Compares the medians of two reports of the performance suite. Usage:
//   bun run compare-perf before.json after.json

import { readFileSync } from 'node:fs'
import type { PerfReport } from '../src/lib/test-support/perf'

const [beforePath, afterPath] = process.argv.slice(2)
if (!beforePath || !afterPath) {
  console.error('Usage: bun run compare-perf <before.json> <after.json>')
  process.exit(1)
}
const read = (path: string): PerfReport => JSON.parse(readFileSync(path, 'utf8'))
const before = read(beforePath)
const after = read(afterPath)

const beforeMedians = new Map(before.results.map((r) => [r.name, r.medianMs]))
const rows = after.results.map((r) => {
  const old = beforeMedians.get(r.name)
  const change = old === undefined ? 'new' : `${(((r.medianMs - old) / old) * 100).toFixed(1)}%`
  return {
    measurement: r.name,
    'before ms': old?.toFixed(1) ?? '-',
    'after ms': r.medianMs.toFixed(1),
    change
  }
})
console.table(rows)
