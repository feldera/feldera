import type { PipelineStatus } from '$lib/services/pipelineManager'
import { match, P } from 'ts-pattern'

export interface StatusColors {
  chip: string
  dot: string
  deploymentChip: string
}

export const pipelineStatusColor = (status: PipelineStatus): StatusColors => {
  return match(status)
    .with('Stopped', () => ({
      chip: 'bg-surface-100-900',
      dot: 'preset-filled-surface-400-600',
      deploymentChip: ''
    }))
    .with('Preparing', 'Provisioning', 'Initializing', () => ({
      chip: 'preset-filled-tertiary-200-800',
      dot: 'preset-filled-tertiary-200-800',
      deploymentChip: 'preset-filled-tertiary-200-800'
    }))
    .with('Paused', () => ({
      chip: 'bg-blue-200 dark:bg-blue-800',
      dot: 'bg-blue-400 dark:bg-blue-600',
      deploymentChip: 'bg-blue-200 dark:bg-blue-800'
    }))
    .with('Standby', () => ({
      chip: 'bg-blue-200 dark:bg-blue-800',
      dot: 'preset-filled-secondary-200-800',
      deploymentChip: 'preset-filled-secondary-200-800'
    }))
    .with('Bootstrapping', () => ({
      chip: 'bg-blue-200 dark:bg-blue-800',
      dot: 'preset-filled-secondary-200-800',
      deploymentChip: 'preset-filled-secondary-200-800'
    }))
    .with('Replaying', () => ({
      chip: 'bg-blue-200 dark:bg-blue-800',
      dot: 'preset-filled-secondary-200-800',
      deploymentChip: 'preset-filled-secondary-200-800'
    }))
    .with('Running', () => ({
      chip: 'preset-filled-success-200-800',
      dot: 'preset-filled-success-400-600',
      deploymentChip: 'preset-tonal-success'
    }))
    .with('Pausing', () => ({
      chip: 'preset-filled-secondary-200-800',
      dot: 'preset-filled-secondary-200-800',
      deploymentChip: 'preset-filled-secondary-200-800'
    }))
    .with('Resuming', () => ({
      chip: 'preset-filled-tertiary-200-800',
      dot: 'preset-filled-tertiary-200-800',
      deploymentChip: 'preset-filled-tertiary-200-800'
    }))
    .with('Stopping', () => ({
      chip: 'preset-filled-secondary-200-800',
      dot: 'preset-filled-secondary-200-800',
      deploymentChip: 'preset-filled-secondary-200-800'
    }))
    .with(
      { Queued: P.any },
      { CompilingSql: P.any },
      { SqlCompiled: P.any },
      { CompilingRust: P.any },
      () => ({
        chip: 'preset-filled-warning-200-800',
        dot: 'preset-filled-warning-400-600',
        deploymentChip: ''
      })
    )
    .with('Unavailable', () => ({
      chip: 'bg-orange-200 dark:bg-orange-800',
      dot: 'bg-orange-300 dark:bg-orange-700',
      deploymentChip: 'bg-orange-300 dark:bg-orange-700'
    }))
    .with('SqlError', 'RustError', 'SystemError', () => ({
      chip: 'preset-filled-error-50-950',
      dot: 'preset-filled-error-400-600',
      deploymentChip: ''
    }))
    .exhaustive()
}
