import type { PipelineStatus } from "$lib/services/pipelineManager"
import { P, match } from "ts-pattern"

export const getStatusLabel = (status: PipelineStatus) => {
  return match(status)
    .with('Shutdown', () => 'Shutdown')
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
