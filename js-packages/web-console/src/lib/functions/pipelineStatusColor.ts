import { match, P } from 'ts-pattern'
import { type StatusColors, type StatusTone, statusToneColors } from 'common-ui/statusTone'
import type { PipelineStatus } from '$lib/services/pipelineManager'

export {
  type StatusColors,
  type StatusTone,
  statusChipClass,
  statusCounterClass,
  statusToneColors
} from 'common-ui/statusTone'

export const pipelineStatusTone = (status: PipelineStatus): StatusTone =>
  match(status)
    .returnType<StatusTone>()
    .with('Stopped', 'Stopping', 'Suspending', 'Suspended', () => 'neutral')
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      () => 'warning'
    )
    .with(
      'Preparing',
      'Provisioning',
      'Initializing',
      'Pausing',
      'Paused',
      'Resuming',
      'Standby',
      'Bootstrapping',
      'Replaying',
      'ConcurrentBootstrapping',
      'Synchronizing',
      () => 'blue'
    )
    .with('Running', () => 'jade')
    .with('AwaitingApproval', 'Unavailable', () => 'warning')
    .with('SqlError', 'RustError', 'SystemError', () => 'error')
    .exhaustive()

export const pipelineStatusColor = (status: PipelineStatus): StatusColors =>
  statusToneColors[pipelineStatusTone(status)]
