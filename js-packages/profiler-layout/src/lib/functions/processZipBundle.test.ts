import type { ZipItem } from 'but-unzip'
import { describe, expect, it } from 'vitest'
import { processProfileFiles } from './processZipBundle.js'

/** Build a ZipItem whose `read()` returns the UTF-8 bytes of `text`. */
function file(filename: string, text: string): ZipItem {
  return { filename, comment: '', read: () => new TextEncoder().encode(text) }
}

const TIMESTAMP = '2026-05-25T12-34-56.789Z'

/** A minimal circuit profile; `processProfileFiles` requires this file to exist. */
function circuitProfile(): ZipItem {
  return file(`${TIMESTAMP}/circuit_profile.json`, JSON.stringify({ nodes: [] }))
}

/** A `pipeline_config.json` with the given extra top-level fields alongside `program_code`. */
function pipelineConfig(extra: Record<string, unknown>): ZipItem {
  return file(
    `${TIMESTAMP}/pipeline_config.json`,
    JSON.stringify({ program_code: 'SELECT 1;', name: 'p', ...extra })
  )
}

describe('processProfileFiles runtimeConfig', () => {
  it('extracts runtime_config from pipeline_config.json', async () => {
    const runtime_config = { workers: 8, storage: { backend: { name: 'default' } } }
    const result = await processProfileFiles([circuitProfile(), pipelineConfig({ runtime_config })])
    expect(result.runtimeConfig).toEqual(runtime_config)
  })

  it('leaves runtimeConfig undefined when the config lacks runtime_config', async () => {
    const result = await processProfileFiles([circuitProfile(), pipelineConfig({})])
    expect(result.runtimeConfig).toBeUndefined()
  })

  it('leaves runtimeConfig undefined when the bundle has no pipeline_config.json', async () => {
    const result = await processProfileFiles([circuitProfile()])
    expect(result.runtimeConfig).toBeUndefined()
  })
})
