/**
 * Unit tests for the health status styles of the health views. Each status takes its colors
 * from the `health*` fields of a status tone.
 */
import { type HealthStatus, healthSeverity } from 'common-ui/healthEvents'
import {
  healthStatusColors,
  healthStatusIcon,
  healthStatusLabel,
  healthStatusTone
} from 'common-ui/healthStatus'
import { type StatusTone, statusToneColors } from 'common-ui/statusTone'
import { describe, expect, it } from 'vitest'

const healthStatuses = Object.keys(healthSeverity) as HealthStatus[]

const expectedTones: [HealthStatus, StatusTone][] = [
  ['idle', 'neutral'],
  ['transitioning', 'blue'],
  ['healthy', 'jade'],
  ['unhealthy', 'warning'],
  ['major_issue', 'error']
]

describe('healthStatusTone', () => {
  it.each(expectedTones)('maps %s to the %s tone', (status, tone) => {
    expect(healthStatusTone(status)).toBe(tone)
  })

  it('covers every health status', () => {
    expect(expectedTones.map(([status]) => status).sort()).toEqual([...healthStatuses].sort())
  })
})

describe('healthStatusColors', () => {
  // The same object, not a copy, so that a health view cannot define its own tone colors.
  it.each(expectedTones)('uses the %s tone colors of the shared table', (status, tone) => {
    expect(healthStatusColors(status)).toBe(statusToneColors[tone])
  })
})

describe('healthStatusLabel and healthStatusIcon', () => {
  it.each([
    ['labels', healthStatusLabel],
    ['icons', healthStatusIcon]
  ] as const)('gives each status different %s', (_, describeStatus) => {
    const values = healthStatuses.map(describeStatus)
    expect(new Set(values).size).toBe(healthStatuses.length)
  })

  it('names a status for the user', () => {
    expect(healthStatusLabel('healthy')).toBe('Operational')
    expect(healthStatusLabel('major_issue')).toBe('Major Issue')
  })

  it('returns an icon without a color, so that the caller picks the color', () => {
    for (const status of healthStatuses) {
      expect(healthStatusIcon(status)).toMatch(/^fd fd-[a-z-]+$/)
    }
  })
})
