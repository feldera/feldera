import { describe, expect, it } from 'vitest'
import type { Configuration, LicenseInformation } from '$lib/services/manager'
import { getLicenseMessage } from './license'

const now = new Date('2026-01-10T00:00:00Z')

const configWithLicense = (license: Partial<LicenseInformation>) =>
  ({
    edition: 'Enterprise',
    license_validity: {
      Exists: {
        current: now.toISOString(),
        description_html: '',
        is_trial: false,
        remind_schedule: 'Session',
        valid_until: null,
        remind_starting_at: null,
        ...license
      }
    }
  }) as Configuration

describe('getLicenseMessage', () => {
  it('shows nothing for a production license far from expiry', () => {
    expect(getLicenseMessage(configWithLicense({}), now)).toBeNull()
    expect(getLicenseMessage(configWithLicense({ is_dev: false }), now)).toBeNull()
  })

  it('shows a permanent development banner for a development license', () => {
    const message = getLicenseMessage(configWithLicense({ is_dev: true }), now)
    expect(message).toMatchObject({ id: 'license_Enterprise_dev', dismissable: 'never' })
    expect(message?.text).toContain('for development purposes only')
  })

  it('keeps the expiry warning of an expiring development license', () => {
    const message = getLicenseMessage(
      configWithLicense({
        is_dev: true,
        valid_until: '2026-01-15T00:00:00Z',
        remind_starting_at: '2026-01-01T00:00:00Z'
      }),
      now
    )
    expect(message?.id).toBe('license_Enterprise_dev')
    expect(message?.text).toContain('for development purposes only')
    expect(message?.text).toContain('license expires')
  })
})
