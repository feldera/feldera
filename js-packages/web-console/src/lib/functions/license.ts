import type { SystemMessage } from '$lib/compositions/initSystemMessages'
import type { Configuration, DisplaySchedule } from '$lib/services/manager'
import { match, P } from 'ts-pattern'
import Dayjs from 'dayjs'

export const displayScheduleToDismissable = (schedule: DisplaySchedule) =>
  match(schedule)
    .with('Once', () => 'once' as const)
    .with('Session', () => 'session' as const)
    .with({ Every: P.select() }, ({ seconds }) => ({ forMs: seconds * 1000 }))
    .with('Always', () => 'never' as const)
    .exhaustive()

export const getLicenseMessage = (config: Configuration, now: Date): SystemMessage | null => {
  {
    const license =
      config.license_validity && 'Exists' in config.license_validity
        ? config.license_validity.Exists
        : undefined

    if (license) {
      if (
        license.valid_until &&
        new Date(license.valid_until) > now &&
        license.remind_starting_at &&
        new Date(license.remind_starting_at) <= now
      ) {
        // Expiring license
        const expiresAt = Dayjs(license.valid_until)
        const time = {
          in: `{toDaysHoursFromNow ${new Date(license.valid_until).valueOf()}}`,
          // at: expiresAt.format('h A'),
          on: expiresAt.format('MMM D')
        }
        return {
          id: `license_${config.edition}_expiring`,
          dismissable: displayScheduleToDismissable(license.remind_schedule),
          text:
            (license.is_trial
              ? `Your trial ends in ${time.in} on ${time.on}`
              : `Your Feldera ${config.edition} license expires in ${time.in} on ${time.on}`) +
            (license.description_html ? `. ${license.description_html}` : ''),
          action: license.extension_url
            ? {
                text: license.is_trial ? 'Upgrade Subscription' : 'Extend Your License',
                href: license.extension_url
              }
            : undefined
        }
      } else if (license.valid_until && new Date(license.valid_until) <= now) {
        // Expired license
        return {
          id: `license_${config.edition}_expired`,
          dismissable: 'never',
          text:
            (license.is_trial
              ? 'Your trial period has ended. Contact us to continue using Feldera'
              : `Your Feldera ${config.edition} license has expired`) +
            (license.description_html ? `. ${license.description_html}` : ''),
          action: license.extension_url
            ? {
                text: license.is_trial ? 'Upgrade Subscription' : 'Extend Your License',
                href: license.extension_url
              }
            : undefined
        }
      }
    }
  }
  {
    const licenseError =
      config.license_validity && 'DoesNotExistOrNotConfirmed' in config.license_validity
        ? config.license_validity.DoesNotExistOrNotConfirmed
        : undefined
    if (licenseError) {
      return {
        id: `license_${config.edition}_error`,
        dismissable: 'never',
        text: licenseError
      }
    }
  }
  return null
}
