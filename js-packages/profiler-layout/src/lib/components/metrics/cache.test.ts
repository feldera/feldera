import { describe, expect, it } from 'vitest'
import { buildCacheTiles, isCacheFamilyMetric } from './cache.js'
import type { MetricGroup } from './dispatch.js'
import { metricKind } from './kind.js'

function group(baseId: string): MetricGroup {
  return { baseId, label: baseId, kind: metricKind(baseId), rows: [] }
}

describe('isCacheFamilyMetric', () => {
  it('matches the six hit/miss/hit-rate cache metrics', () => {
    for (const prefix of ['foreground', 'background']) {
      for (const suffix of ['hits', 'misses', 'hit_rate_percent']) {
        expect(isCacheFamilyMetric(`${prefix}_cache_${suffix}`)).toBe(true)
      }
    }
  })

  it('excludes occupancy and unrelated metrics', () => {
    expect(isCacheFamilyMetric('foreground_cache_occupancy')).toBe(false)
    expect(isCacheFamilyMetric('background_cache_occupancy')).toBe(false)
    expect(isCacheFamilyMetric('used_memory_bytes')).toBe(false)
  })
})

describe('buildCacheTiles', () => {
  it('assembles one tile per cache, foreground first, with hits/misses/hit-rate assigned', () => {
    const tiles = buildCacheTiles([
      group('background_cache_misses'),
      group('foreground_cache_hits'),
      group('background_cache_hit_rate_percent'),
      group('foreground_cache_hit_rate_percent'),
      group('foreground_cache_misses'),
      group('background_cache_hits'),
      group('foreground_cache_occupancy') // ignored
    ])
    expect(tiles.map((t) => t.prefix)).toEqual(['foreground', 'background'])
    const fg = tiles[0]!
    expect(fg.title).toBe('Foreground cache')
    expect(fg.hits?.baseId).toBe('foreground_cache_hits')
    expect(fg.misses?.baseId).toBe('foreground_cache_misses')
    expect(fg.hitRate?.baseId).toBe('foreground_cache_hit_rate_percent')
  })

  it('returns no tiles when the family is absent', () => {
    expect(buildCacheTiles([group('used_memory_bytes')])).toEqual([])
  })
})
