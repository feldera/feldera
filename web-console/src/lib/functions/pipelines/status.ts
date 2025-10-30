import type { PipelineStatus } from '$lib/services/pipelineManager'
import { P, match } from 'ts-pattern'

export const getPipelineStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Stopped', () => 'Ready To Start')
    .with('Preparing', () => 'Preparing')
    .with('Provisioning', () => 'Provisioning')
    .with('Initializing', () => 'Initializing')
    .with('Paused', () => 'Paused')
    .with('Standby', () => 'Standby')
    .with('Bootstrapping', () => 'Bootstrapping')
    .with('Replaying', () => 'Replaying')
    .with('AwaitingApproval', () => 'Awaiting Approval')
    .with('Running', () => 'Running')
    .with('Pausing', () => 'Pausing')
    .with('Resuming', () => 'Resuming')
    .with('Suspending', () => 'Stopping')
    .with('Suspended', () => 'Stopping')
    .with('Stopping', () => 'Stopping')
    .with({ Queued: P.any }, () => 'Queued')
    .with({ CompilingSql: P.any }, () => 'Compiling SQL')
    .with({ SqlCompiled: P.any }, () => 'SQL Compiled')
    .with({ CompilingRust: P.any }, () => 'Compiling Rust')
    .with('Unavailable', () => 'Unavailable')
    .with('SqlError', () => 'Program Error')
    .with('RustError', () => 'Program Error')
    .with('SystemError', () => 'Program Error')
    .exhaustive()
}

export const getDeploymentStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Stopped', () => '')
    .with('Preparing', () => 'Preparing')
    .with('Provisioning', () => 'Provisioning')
    .with('Initializing', () => 'Initializing')
    .with('Paused', () => 'Paused')
    .with('Suspending', () => 'Stopping')
    .with('Suspended', () => 'Stopping')
    .with('Standby', () => 'Standby')
    .with('Bootstrapping', () => 'Bootstrapping')
    .with('Replaying', () => 'Replaying')
    .with('AwaitingApproval', () => 'Awaiting Approval')
    .with('Running', () => 'Running')
    .with('Pausing', () => 'Pausing')
    .with('Resuming', () => 'Resuming')
    .with('Stopping', () => 'Stopping')
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      () => ''
    )
    .with('Unavailable', () => 'Unavailable')
    .with('SqlError', () => '')
    .with('RustError', () => '')
    .with('SystemError', () => '')
    .exhaustive()
}

/**
 * Is the pipeline ready to process API requests related to data processing
 */
export const isPipelineInteractive = (status: PipelineStatus) => {
  return match(status)
    .with('Stopped', () => false)
    .with('Preparing', () => false)
    .with('Provisioning', () => false)
    .with('Initializing', () => false)
    .with('Paused', () => true)
    .with('Suspending', () => false)
    .with('Suspended', () => false)
    .with('Standby', () => false)
    .with('Bootstrapping', () => false)
    .with('Replaying', () => false)
    .with('AwaitingApproval', () => false)
    .with('Running', () => true)
    .with('Pausing', () => true)
    .with('Resuming', () => true)
    .with('Stopping', () => false)
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      () => false
    )
    .with('Unavailable', () => false)
    .with('SqlError', () => false)
    .with('RustError', () => false)
    .with('SystemError', () => false)
    .exhaustive()
}

export const isPipelineCodeEditable = (status: PipelineStatus) => {
  return match(status)
    .with('Stopped', () => true)
    .with('Preparing', () => false)
    .with('Provisioning', () => false)
    .with('Initializing', () => false)
    .with('Paused', () => false)
    .with('Suspending', () => false)
    .with('Suspended', () => false)
    .with('Standby', () => false)
    .with('Bootstrapping', () => false)
    .with('Replaying', () => false)
    .with('AwaitingApproval', () => false)
    .with('Running', () => false)
    .with('Pausing', () => false)
    .with('Resuming', () => false)
    .with('Stopping', () => false)
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      (cause) => Object.values(cause)[0].cause === 'compile'
    )
    .with('Unavailable', () => false)
    .with('SqlError', () => true)
    .with('RustError', () => true)
    .with('SystemError', () => true)
    .exhaustive()
}

export const isPipelineShutdown = (status: PipelineStatus) => {
  return match(status)
    .with('Stopped', () => true)
    .with('Preparing', () => false)
    .with('Provisioning', () => false)
    .with('Initializing', () => false)
    .with('Paused', () => false)
    .with('Suspending', () => false)
    .with('Suspended', () => false)
    .with('Standby', () => false)
    .with('Bootstrapping', () => false)
    .with('Replaying', () => false)
    .with('AwaitingApproval', () => false)
    .with('Running', () => false)
    .with('Pausing', () => false)
    .with('Resuming', () => false)
    .with('Stopping', () => false)
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      () => true
    )
    .with('Unavailable', () => false)
    .with('SqlError', () => true)
    .with('RustError', () => true)
    .with('SystemError', () => true)
    .exhaustive()
}

export const isMetricsAvailable = (status: PipelineStatus) => {
  return match(status)
    .returnType<'yes' | 'no' | 'soon' | 'missing'>()
    .with('Stopped', () => 'no')
    .with('Preparing', () => 'soon')
    .with('Provisioning', () => 'soon')
    .with('Initializing', () => 'soon')
    .with('Paused', () => 'yes')
    .with('Suspending', () => 'yes')
    .with('Suspended', () => 'yes')
    .with('Standby', () => 'yes')
    .with('Bootstrapping', () => 'yes')
    .with('Replaying', () => 'yes')
    .with('AwaitingApproval', () => 'soon')
    .with('Running', () => 'yes')
    .with('Pausing', () => 'yes')
    .with('Resuming', () => 'yes')
    .with('Stopping', () => 'no')
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      () => 'no'
    )
    .with('Unavailable', () => 'missing')
    .with('SqlError', () => 'no')
    .with('RustError', () => 'no')
    .with('SystemError', () => 'no')
    .exhaustive()
}
