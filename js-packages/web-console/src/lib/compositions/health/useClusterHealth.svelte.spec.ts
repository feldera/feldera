/**
 * The store keeps the cluster's status in module scope, shared by every consumer. These
 * check what it reports before the first poll has answered.
 */
import { describe, expect, it } from 'vitest'
import { useClusterHealth } from './useClusterHealth.svelte'

describe('useClusterHealth', () => {
  it('claims neither data nor staleness before the first poll', () => {
    const health = useClusterHealth()
    expect(health.current.recordedAt).toBeNull()
    expect(health.current.stale).toBe(false)
  })

  it('reports a status for every service the console shows', () => {
    const { api, compiler, runner } = useClusterHealth().current
    expect([api, compiler, runner]).toEqual(['healthy', 'healthy', 'healthy'])
  })
})
