/**
 * Checks that only `common-ui/src/lib/statusTone.ts` defines the status tone colors. A class such
 * as `bg-status-jade-dot` in another file is a second definition of a tone color, and it can
 * drift from `statusToneColors`.
 */
import { readdirSync, readFileSync } from 'node:fs'
import { join, relative } from 'node:path'
import { describe, expect, it } from 'vitest'

const jsPackages = join(import.meta.dirname, '../../../..')
const sourceDirs = ['common-ui/src', 'web-console/src', 'profiler-layout/src', 'profiler-app/src']
const toneTable = 'common-ui/src/lib/statusTone.ts'
const toneClass = /\b(?:bg|text|fill|border|stroke)-status-(?:neutral|blue|jade|warning|error)\b/

/** Lists the source files under `dir`, without tests. */
function sourceFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true, recursive: true })
    .filter((entry) => entry.isFile() && /\.(svelte|ts|js)$/.test(entry.name))
    .filter((entry) => !/\.(spec|test)\.ts$/.test(entry.name))
    .map((entry) => join(entry.parentPath, entry.name))
    .filter((path) => !path.includes('node_modules'))
}

describe('status tone colors', () => {
  it(`are spelled out only in ${toneTable}`, () => {
    const offenders = sourceDirs
      .flatMap((dir) => sourceFiles(join(jsPackages, dir)))
      .map((path) => relative(jsPackages, path))
      .filter((path) => path !== toneTable)
      .filter((path) => toneClass.test(readFileSync(join(jsPackages, path), 'utf8')))
    expect(offenders).toEqual([])
  })

  it(`finds the colors in ${toneTable}`, () => {
    expect(readFileSync(join(jsPackages, toneTable), 'utf8')).toMatch(toneClass)
  })
})
