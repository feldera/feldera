import { describe, expect, it } from 'vitest'
import { type HealthStatus, healthSeverity } from 'common-ui/healthEvents'

describe('healthSeverity', () => {
  it('orders the statuses from idle to a major issue', () => {
    const statuses = Object.keys(healthSeverity) as HealthStatus[]
    const bySeverity = statuses.sort((a, b) => healthSeverity[a] - healthSeverity[b])
    expect(bySeverity).toEqual(['idle', 'healthy', 'transitioning', 'unhealthy', 'major_issue'])
  })
})
