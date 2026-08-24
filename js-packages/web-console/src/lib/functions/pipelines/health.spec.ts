import { describe, expect, it } from 'vitest'
import { clusterHealthMessage, staleMonitoringMessage, worstClusterStatus } from './health'

describe('staleMonitoringMessage', () => {
  const since = new Date('2026-05-01T12:00:00Z')

  it('names the runner only where it is a separate process', () => {
    expect(staleMonitoringMessage(true)).toContain('Kubernetes runner')
    // In the open-source edition the monitor is a task within the process that just
    // answered, so pointing at the runner would misdirect.
    expect(staleMonitoringMessage(false)).not.toContain('Kubernetes runner')
  })

  it('says the statuses shown are not current, whatever the edition', () => {
    for (const message of [staleMonitoringMessage(true), staleMonitoringMessage(false)]) {
      expect(message).toContain('last recorded ones, not current ones')
    }
  })

  it('dates the last data when given a timestamp', () => {
    expect(staleMonitoringMessage(false, since)).toContain(since.toLocaleString())
    expect(staleMonitoringMessage(false)).toContain('No new cluster monitoring data.')
  })
})

describe('clusterHealthMessage', () => {
  const healthy = { stale: false, api: 'healthy', compiler: 'healthy', runner: 'healthy' } as const

  it('says nothing while every service is healthy and the data is fresh', () => {
    expect(clusterHealthMessage(healthy, true)).toBeNull()
  })

  it('names the service that is unhealthy', () => {
    expect(clusterHealthMessage({ ...healthy, compiler: 'unhealthy' }, true)).toBe(
      'There is an issue with the compiler server.'
    )
  })

  it('leads with staleness but keeps the issue the last data recorded', () => {
    const message = clusterHealthMessage({ ...healthy, stale: true, runner: 'major_issue' }, true)
    expect(message).toContain(staleMonitoringMessage(true))
    expect(message).toContain('already showed an issue with the runner')
  })

  it('reports staleness alone when the last data recorded no issue', () => {
    expect(clusterHealthMessage({ ...healthy, stale: true }, true)).toBe(
      staleMonitoringMessage(true)
    )
  })
})

describe('worstClusterStatus', () => {
  it('is healthy only when every status is', () => {
    expect(worstClusterStatus(['healthy', 'healthy'])).toBe('healthy')
    expect(worstClusterStatus(['healthy', 'unhealthy'])).toBe('unhealthy')
    expect(worstClusterStatus(['major_issue', 'unhealthy'])).toBe('major_issue')
  })
})
