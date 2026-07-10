import type { ZipItem } from 'but-unzip'
import { describe, expect, it } from 'vitest'
import { processProfileFiles, selectProfiles } from './processZipBundle.js'

/** Build a ZipItem whose `read()` returns the UTF-8 bytes of `text`. */
function file(filename: string, text: string): ZipItem {
  return { filename, comment: '', read: () => new TextEncoder().encode(text) }
}

const TIMESTAMP = '2026-05-25T12-34-56.789Z'

/** A minimal circuit profile. */
function circuitProfile(dir = TIMESTAMP): ZipItem {
  return file(`${dir}/circuit_profile.json`, JSON.stringify({ nodes: [] }))
}

/** A `pipeline_config.json` with the given extra top-level fields alongside `program_code`. */
function pipelineConfig(extra: Record<string, unknown>, dir = TIMESTAMP): ZipItem {
  return file(
    `${dir}/pipeline_config.json`,
    JSON.stringify({ program_code: 'SELECT 1;', name: 'p', ...extra })
  )
}

function logs(dir = TIMESTAMP): ZipItem {
  return file(`${dir}/logs.txt`, 'starting up\n')
}

describe('selectProfiles', () => {
  it('keeps a collection that has no circuit profile but does have other files', () => {
    // Regression: the old filter required a circuit_profile.json, so a config/logs-only bundle
    // was dropped and refused to open.
    const collections = selectProfiles([pipelineConfig({}), logs()])
    expect(collections).toHaveLength(1)
    expect(collections[0][1]).toHaveLength(2)
  })

  it('drops files that carry no recognised content', () => {
    const collections = selectProfiles([
      file(`${TIMESTAMP}/unknown.bin`, 'x'),
      file('README.txt', 'not in a collection dir')
    ])
    expect(collections).toEqual([])
  })

  it('sorts multiple collections oldest-first', () => {
    const older = '2026-05-25T12-00-00.000Z'
    const newer = '2026-05-25T13-00-00.000Z'
    const collections = selectProfiles([circuitProfile(newer), logs(older)])
    expect(collections.map(([d]) => d.getTime())).toEqual([
      new Date('2026-05-25T12:00:00.000Z').getTime(),
      new Date('2026-05-25T13:00:00.000Z').getTime()
    ])
  })
})

describe('processProfileFiles without a circuit profile', () => {
  it('leaves profile undefined while still extracting config and logs', async () => {
    const result = await processProfileFiles([pipelineConfig({}), logs()])
    expect(result.profile).toBeUndefined()
    expect(result.sources).toEqual(['SELECT 1;'])
    expect(result.pipelineName).toBe('p')
    expect(result.logText).toBe('starting up\n')
  })
})

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
