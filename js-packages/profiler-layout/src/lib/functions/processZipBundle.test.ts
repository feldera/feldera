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

/** Build a ZipItem whose `read()` returns `bytes`. */
function binaryFile(filename: string, bytes: Uint8Array): ZipItem {
  return { filename, comment: '', read: () => bytes }
}

async function gzip(text: string): Promise<Uint8Array> {
  const compressed = new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'))
  return new Uint8Array(await new Response(compressed).arrayBuffer())
}

describe('processProfileFiles with a gzip-compressed circuit profile', () => {
  const profile = { metrics: [], worker_profiles: [], graph: { nodes: [], edges: [] } }

  it('recognises circuit_profile.json.gz as a collection file', async () => {
    const collections = selectProfiles([
      binaryFile(`${TIMESTAMP}/circuit_profile.json.gz`, await gzip('{}'))
    ])
    expect(collections).toHaveLength(1)
  })

  it('inflates circuit_profile.json.gz before parsing it', async () => {
    const result = await processProfileFiles([
      binaryFile(`${TIMESTAMP}/circuit_profile.json.gz`, await gzip(JSON.stringify(profile)))
    ])
    expect(result.profile).toEqual(profile)
  })

  it('still parses a plain circuit_profile.json', async () => {
    const result = await processProfileFiles([
      file(`${TIMESTAMP}/circuit_profile.json`, JSON.stringify(profile))
    ])
    expect(result.profile).toEqual(profile)
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

/** A pipeline monitor event with the fields that the bundle has, recorded at `recordedAt`. */
function monitorEvent(eventId: string, recordedAt = '2026-05-25T12:30:00Z') {
  return {
    event_id: eventId,
    recorded_at: recordedAt,
    program_status: 'Success',
    storage_status: 'InUse',
    deployment_has_error: false,
    deployment_resources_status: 'Provisioned',
    deployment_resources_desired_status: 'Provisioned',
    deployment_runtime_status: 'Running'
  }
}

function pipelineEvents(text: string, dir = TIMESTAMP): ZipItem {
  return file(`${dir}/pipeline_events.json`, text)
}

describe('processProfileFiles pipelineEvents', () => {
  it('extracts the events from pipeline_events.json in their order', async () => {
    const events = [
      monitorEvent('e2', '2026-05-25T12:30:00Z'),
      monitorEvent('e1', '2026-05-25T12:00:00Z')
    ]
    const result = await processProfileFiles([pipelineEvents(JSON.stringify(events))])
    expect(result.pipelineEvents).toEqual(events)
  })

  it('extracts an empty event list', async () => {
    const result = await processProfileFiles([pipelineEvents('[]')])
    expect(result.pipelineEvents).toEqual([])
  })

  it('leaves pipelineEvents undefined when the bundle has no pipeline_events.json', async () => {
    const result = await processProfileFiles([circuitProfile()])
    expect(result.pipelineEvents).toBeUndefined()
  })

  it.each([
    ['is not valid JSON', '[{'],
    ['is an object', '{"events": []}'],
    ['is null', 'null']
  ])('leaves pipelineEvents undefined and loads the rest when the file %s', async (_, text) => {
    const result = await processProfileFiles([logs(), pipelineEvents(text)])
    expect(result.pipelineEvents).toBeUndefined()
    expect(result.logText).toBe('starting up\n')
  })

  it('skips the entries that have no event ID or recording time', async () => {
    const valid = monitorEvent('e1')
    const entries = [
      null,
      7,
      'e0',
      { ...valid, event_id: 5 },
      { ...valid, recorded_at: null },
      { event_id: 'e9' },
      valid
    ]
    const result = await processProfileFiles([pipelineEvents(JSON.stringify(entries))])
    expect(result.pipelineEvents).toEqual([valid])
  })
})

describe('selectProfiles with pipeline events', () => {
  it('keeps a collection that has only pipeline_events.json', () => {
    const profiles = selectProfiles([pipelineEvents('[]')])
    expect(profiles).toHaveLength(1)
    expect(profiles[0][1].map((f) => f.filename)).toEqual([`${TIMESTAMP}/pipeline_events.json`])
  })
})
