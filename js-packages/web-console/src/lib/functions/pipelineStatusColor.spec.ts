/**
 * Unit tests for the pipeline status colour mapping. Every status must land in
 * one of the five Figma tones, and every tone must resolve to classes built
 * from the `--color-status-<tone>` theme colours.
 */
import { describe, expect, it } from 'vitest'
import type { PipelineStatus } from '$lib/services/pipelineManager'
import {
  pipelineStatusColor,
  pipelineStatusTone,
  type StatusTone,
  statusToneColors
} from './pipelineStatusColor'

const expectedTones: [PipelineStatus, StatusTone][] = [
  ['Stopped', 'neutral'],
  ['Stopping', 'neutral'],
  ['Suspending', 'neutral'],
  ['Suspended', 'neutral'],
  [{ Queued: { cause: 'compile' } }, 'warning'],
  [{ CompilingSql: { cause: 'compile' } }, 'warning'],
  [{ SqlCompiled: { cause: 'upgrade' } }, 'warning'],
  [{ CompilingRust: { cause: 'upgrade' } }, 'warning'],
  ['Preparing', 'blue'],
  ['Provisioning', 'blue'],
  ['Initializing', 'blue'],
  ['Pausing', 'blue'],
  ['Paused', 'blue'],
  ['Resuming', 'blue'],
  ['Standby', 'blue'],
  ['Bootstrapping', 'blue'],
  ['Replaying', 'blue'],
  ['ConcurrentBootstrapping', 'blue'],
  ['Synchronizing', 'blue'],
  ['Running', 'jade'],
  ['AwaitingApproval', 'warning'],
  ['Unavailable', 'warning'],
  ['SqlError', 'error'],
  ['RustError', 'error'],
  ['SystemError', 'error']
]

describe('pipelineStatusTone', () => {
  it.each(expectedTones)('maps %j to %s', (status, tone) => {
    expect(pipelineStatusTone(status)).toBe(tone)
  })

  it('ignores the compilation cause', () => {
    expect(pipelineStatusTone({ Queued: { cause: 'upgrade' } })).toBe(
      pipelineStatusTone({ Queued: { cause: 'compile' } })
    )
  })
})

describe('pipelineStatusColor', () => {
  it('returns the classes of the status tone', () => {
    for (const [status, tone] of expectedTones) {
      expect(pipelineStatusColor(status)).toBe(statusToneColors[tone])
    }
  })
})

describe('statusToneColors', () => {
  it.each(Object.entries(statusToneColors))(
    'builds %s classes from its theme colours',
    (tone, colors) => {
      expect(colors.chip).toBe(`bg-status-${tone}-subtle text-status-${tone}`)
      expect(colors.dot).toMatch(new RegExp(`^bg-status-${tone}(-dot)?$`))
    }
  )
})
