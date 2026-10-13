// The tone, label and icon of each health status. The health views take their colors from the
// `health*` fields of the tone table in `statusTone.ts`.

import type { HealthStatus } from './healthEvents'
import { match } from 'ts-pattern'
import { type StatusColors, type StatusTone, statusToneColors } from './statusTone'

export const healthStatusTone = (status: HealthStatus): StatusTone =>
  match(status)
    .returnType<StatusTone>()
    .with('idle', () => 'neutral')
    .with('transitioning', () => 'blue')
    .with('healthy', () => 'jade')
    .with('unhealthy', () => 'warning')
    .with('major_issue', () => 'error')
    .exhaustive()

export const healthStatusLabel = (status: HealthStatus): string =>
  match(status)
    .with('idle', () => 'Idle')
    .with('transitioning', () => 'Transitioning')
    .with('healthy', () => 'Operational')
    .with('unhealthy', () => 'Service degradation')
    .with('major_issue', () => 'Major Issue')
    .exhaustive()

/** The icon class of a status, without a color. */
export const healthStatusIcon = (status: HealthStatus): string =>
  match(status)
    .with('idle', () => 'fd fd-circle-minus')
    .with('transitioning', () => 'fd fd-circle-dot')
    .with('healthy', () => 'fd fd-circle-check-big')
    .with('unhealthy', () => 'fd fd-triangle-alert')
    .with('major_issue', () => 'fd fd-circle-x')
    .exhaustive()

export const healthStatusColors = (status: HealthStatus): StatusColors =>
  statusToneColors[healthStatusTone(status)]
