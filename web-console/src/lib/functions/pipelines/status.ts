import type { PipelineStatus } from '$lib/services/pipelineManager'
import { P, match } from 'ts-pattern'

export const getPipelineStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', () => 'Ready To Start')
    .with('Preparing', () => 'Preparing')
    .with('Provisioning', () => 'Provisioning')
    .with('Initializing', () => 'Initializing')
    .with('Paused', () => 'Paused')
    .with('Suspended', () => 'Suspended')
    .with('Suspending', () => 'Suspending')
    .with('Running', () => 'Running')
    .with('Pausing', () => 'Pausing')
    .with('Resuming', () => 'Resuming')
    .with('ShuttingDown', () => 'Shutting Down')
    .with({ PipelineError: P._ }, () => 'Pipeline Error')
    .with({ Queued: P.any }, () => 'Queued')
    .with({ CompilingSql: P.any }, () => 'Compiling SQL')
    .with({ SqlCompiled: P.any }, () => 'SQL compiled')
    .with({ CompilingRust: P.any }, () => 'Compiling Rust')
    .with('Unavailable', () => 'Unavailable')
    .with('SqlError', () => 'Program Error')
    .with('RustError', () => 'Program Error')
    .with('SystemError', () => 'Program Error')
    .exhaustive()
}

export const getDeploymentStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', () => '')
    .with('Preparing', () => 'Preparing')
    .with('Provisioning', () => 'Provisioning')
    .with('Initializing', () => 'Initializing')
    .with('Paused', () => 'Paused')
    .with('Suspended', () => 'Suspended')
    .with('Suspending', () => 'Suspending')
    .with('Running', () => 'Running')
    .with('Pausing', () => 'Pausing')
    .with('Resuming', () => 'Resuming')
    .with('ShuttingDown', () => 'Stopping')
    .with({ PipelineError: P._ }, () => 'Pipeline Error')
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
    .with('Shutdown', () => false)
    .with('Preparing', () => false)
    .with('Provisioning', () => false)
    .with('Initializing', () => false)
    .with('Paused', () => true)
    .with('Suspended', () => false)
    .with('Suspending', () => false)
    .with('Running', () => true)
    .with('Pausing', () => true)
    .with('Resuming', () => true)
    .with('ShuttingDown', () => false)
    .with({ PipelineError: P._ }, () => false)
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
    .with('Shutdown', () => true)
    .with('Preparing', () => false)
    .with('Provisioning', () => false)
    .with('Initializing', () => false)
    .with('Paused', () => false)
    .with('Suspended', () => false)
    .with('Suspending', () => false)
    .with('Running', () => false)
    .with('Pausing', () => false)
    .with('Resuming', () => false)
    .with('ShuttingDown', () => false)
    .with({ PipelineError: P._ }, () => false)
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

export const isPipelineConfigEditable = (status: PipelineStatus) =>
  status === 'Suspended' || isPipelineCodeEditable(status)

export const isMetricsAvailable = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', () => 'no' as const)
    .with('Preparing', () => 'soon' as const)
    .with('Provisioning', () => 'soon' as const)
    .with('Initializing', () => 'soon' as const)
    .with('Paused', () => 'yes' as const)
    .with('Suspended', () => 'soon' as const)
    .with('Suspending', () => 'yes' as const)
    .with('Running', () => 'yes' as const)
    .with('Pausing', () => 'yes' as const)
    .with('Resuming', () => 'yes' as const)
    .with('ShuttingDown', () => 'no' as const)
    .with({ PipelineError: P._ }, () => 'no' as const)
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      () => 'no' as const
    )
    .with('Unavailable', () => 'soon' as const)
    .with('SqlError', () => 'no' as const)
    .with('RustError', () => 'no' as const)
    .with('SystemError', () => 'no' as const)
    .exhaustive()
}
