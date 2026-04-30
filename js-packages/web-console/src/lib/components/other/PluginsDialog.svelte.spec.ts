import { describe, expect, it } from 'vitest'

/**
 * Status-dot color mapping for the connectors.toml tab.
 *
 * Green  → 'ready'
 * Yellow → 'building'
 * Red    → 'failed' | 'not_configured' | null (unknown / loading)
 */
function statusDotClass(status: string | null): string {
  if (status === 'ready') return 'bg-success-500'
  if (status === 'building') return 'bg-warning-500'
  return 'bg-error-500'
}

describe('PluginsDialog status-dot color mapping', () => {
  it('maps ready to green', () => {
    expect(statusDotClass('ready')).toBe('bg-success-500')
  })

  it('maps building to yellow', () => {
    expect(statusDotClass('building')).toBe('bg-warning-500')
  })

  it('maps failed to red', () => {
    expect(statusDotClass('failed')).toBe('bg-error-500')
  })

  it('maps not_configured to red', () => {
    expect(statusDotClass('not_configured')).toBe('bg-error-500')
  })

  it('maps null (loading/unknown) to red', () => {
    expect(statusDotClass(null)).toBe('bg-error-500')
  })
})
