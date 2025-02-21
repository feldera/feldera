import type { PipelineStatus } from '$lib/services/pipelineManager'
import type { Arguments, FunctionType } from '$lib/types/common/function'
import { P, match } from 'ts-pattern'

export const getPipelineStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', { SqlWarning: P.any }, () => 'Ready To Start')
    .with('Provisioning', () => 'Provisioning')
    .with('Starting up', () => 'Starting up')
    .with('Paused', () => 'Paused')
    .with('Running', () => 'Running')
    .with('Pausing', () => 'Pausing')
    .with('Resuming', () => 'Resuming')
    .with('ShuttingDown', () => 'Shutting Down')
    .with({ PipelineError: P._ }, () => 'Pipeline Error')
    .with({ Queued: P.any }, () => 'Queued')
    .with({ 'Compiling SQL': P.any }, () => 'Compiling SQL')
    .with({ 'SQL compiled': P.any }, () => 'SQL compiled')
    .with({ 'Compiling binary': P.any }, () => 'Compiling Rust')
    .with('Unavailable', () => 'Unavailable')
    .with({ SqlError: P._ }, () => 'Program Error')
    .with({ RustError: P._ }, () => 'Program Error')
    .with({ SystemError: P._ }, () => 'Program Error')
    .exhaustive()
}

export const getDeploymentStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', { SqlWarning: P.any }, () => '')
    .with('Provisioning', () => 'Provisioning')
    .with('Starting up', () => 'Starting up')
    .with('Paused', () => 'Paused')
    .with('Running', () => 'Running')
    .with('Pausing', () => 'Pausing')
    .with('Resuming', () => 'Resuming')
    .with('ShuttingDown', () => 'Shutting Down')
    .with({ PipelineError: P._ }, () => 'Pipeline Error')
    .with(
      { Queued: P.any },
      { 'Compiling SQL': P.any },
      { 'SQL compiled': P.any },
      { 'Compiling binary': P.any },
      () => ''
    )
    .with('Unavailable', () => 'Unavailable')
    .with({ SqlError: P._ }, () => '')
    .with({ RustError: P._ }, () => '')
    .with({ SystemError: P._ }, () => '')
    .exhaustive()
}

export const isPipelineIdle = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', { SqlWarning: P.any }, () => true)
    .with('Provisioning', () => false)
    .with('Starting up', () => false)
    .with('Paused', () => false)
    .with('Running', () => false)
    .with('Pausing', () => false)
    .with('Resuming', () => false)
    .with('ShuttingDown', () => false)
    .with({ PipelineError: P._ }, () => false)
    .with(
      { Queued: P.any },
      { 'Compiling SQL': P.any },
      { 'SQL compiled': P.any },
      { 'Compiling binary': P.any },
      () => true
    )
    .with('Unavailable', () => false)
    .with({ SqlError: P._ }, () => true)
    .with({ RustError: P._ }, () => true)
    .with({ SystemError: P._ }, () => true)
    .exhaustive()
}

export const isPipelineEditable = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', { SqlWarning: P.any }, () => true)
    .with('Provisioning', () => false)
    .with('Starting up', () => false)
    .with('Paused', () => false)
    .with('Running', () => false)
    .with('Pausing', () => false)
    .with('Resuming', () => false)
    .with('ShuttingDown', () => false)
    .with({ PipelineError: P._ }, () => false)
    .with(
      { Queued: P.any },
      { 'Compiling SQL': P.any },
      { 'SQL compiled': P.any },
      { 'Compiling binary': P.any },
      (cause) => Object.values(cause)[0] === 'compile'
    )
    .with('Unavailable', () => false)
    .with({ SqlError: P._ }, () => true)
    .with({ RustError: P._ }, () => true)
    .with({ SystemError: P._ }, () => true)
    .exhaustive()
}

export const isMetricsAvailable = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', { SqlWarning: P.any }, () => 'no' as const)
    .with('Provisioning', () => 'no' as const)
    .with('Starting up', () => 'no' as const)
    .with('Paused', () => 'yes' as const)
    .with('Running', () => 'yes' as const)
    .with('Pausing', () => 'yes' as const)
    .with('Resuming', () => 'yes' as const)
    .with('ShuttingDown', () => 'no' as const)
    .with({ PipelineError: P._ }, () => 'no' as const)
    .with(
      { Queued: P.any },
      { 'Compiling SQL': P.any },
      { 'SQL compiled': P.any },
      { 'Compiling binary': P.any },
      () => 'no' as const
    )
    .with('Unavailable', () => 'soon' as const)
    .with({ SqlError: P._ }, () => 'no' as const)
    .with({ RustError: P._ }, () => 'no' as const)
    .with({ SystemError: P._ }, () => 'no' as const)
    .exhaustive()
}
