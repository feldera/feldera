import type { PipelineStatus } from '$lib/services/pipelineManager'
import { P, match } from 'ts-pattern'

export const getStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', () => 'Ready To Run')
    .with('Starting up', () => 'Starting up')
    .with('Initializing', () => 'Initializing')
    .with('Paused', () => 'Paused')
    .with('Running', () => 'Running')
    .with('ShuttingDown', () => 'Shutting Dwn')
    .with({ PipelineError: P.select() }, () => 'Pipeline Err')
    .with('Compiling sql', () => 'Compiling sql')
    .with('Queued', () => 'Queued')
    .with('Compiling bin', () => 'Compiling bin')
    .with({ SqlError: P.select() }, () => 'Program err')
    .with({ RustError: P.select() }, () => 'Program err')
    .with({ SystemError: P.select() }, () => 'Program err')
    .exhaustive()
}

export const isPipelineIdle = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', () => true)
    .with('Starting up', () => false)
    .with('Initializing', () => false)
    .with('Paused', () => false)
    .with('Running', () => false)
    .with('ShuttingDown', () => false)
    .with({ PipelineError: P.select() }, () => false)
    .with('Compiling sql', () => true)
    .with('Queued', () => true)
    .with('Compiling bin', () => true)
    .with({ SqlError: P.select() }, () => true)
    .with({ RustError: P.select() }, () => true)
    .with({ SystemError: P.select() }, () => true)
    .exhaustive()
}