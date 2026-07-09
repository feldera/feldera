// Groups the cache metric family into composite tiles. The hits/misses (K8) and hit-rate (K2)
// metrics for each cache are subsumed into one CacheTile per cache (foreground / background)
// rather than rendered as individual widgets. See ./README.md and parts/cache/CacheTile.svelte.
import type { MetricGroup } from './dispatch'

const CACHE_MEMBER = /^(foreground|background)_cache_(hits|misses|hit_rate_percent)$/

/** True when a metric belongs to a cache tile (and so should not render as its own widget). */
export function isCacheFamilyMetric(baseId: string): boolean {
  return CACHE_MEMBER.test(baseId)
}

export type CacheTileData = {
  prefix: string
  title: string
  hits?: MetricGroup
  misses?: MetricGroup
  hitRate?: MetricGroup
}

/** Assemble the cache-family groups in `groups` into one tile per cache, foreground first. */
export function buildCacheTiles(groups: MetricGroup[]): CacheTileData[] {
  const byPrefix = new Map<string, CacheTileData>()
  for (const g of groups) {
    const m = g.baseId.match(CACHE_MEMBER)
    if (!m) {
      continue
    }
    const prefix = m[1]!
    let tile = byPrefix.get(prefix)
    if (!tile) {
      tile = { prefix, title: `${prefix[0]!.toUpperCase()}${prefix.slice(1)} cache` }
      byPrefix.set(prefix, tile)
    }
    if (m[2] === 'hits') {
      tile.hits = g
    } else if (m[2] === 'misses') {
      tile.misses = g
    } else {
      tile.hitRate = g
    }
  }
  return ['foreground', 'background'].flatMap((p) => {
    const t = byPrefix.get(p)
    return t ? [t] : []
  })
}
