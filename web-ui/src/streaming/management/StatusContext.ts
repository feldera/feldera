// Save a `ClientPipelineStatus` for each pipeline.
//
// `ClientPipelineStatus` is a superset of the `PipelineStatus` enum.
// containing some extra state for better UX.
import { create } from 'zustand'

export enum ClientPipelineStatus {
  // Shouldn't happen, means we haven't put it in the map
  UNKNOWN = 'Unknown',
  // Maps to PipelineStatus.SHUTDOWN
  INACTIVE = 'Inactive',
  // Maps to PipelineStatus.PROVISIONING
  PROVISIONING = 'Provisioning …',
  // Maps to PipelineStatus.INITIALIZING
  INITIALIZING = 'Creating …',
  CREATE_FAILURE = 'Create failed',
  // Maps to PipelineStatus.DEPLOYED
  DEPLOYED = 'Deployed',
  STARTING = 'Starting …',
  STARTUP_FAILURE = 'Starting failed',
  // Maps to PipelineStatus.RUNNING
  RUNNING = 'Running',
  PAUSING = 'Pausing …',
  // Maps to PipelineStatus.PAUSED
  PAUSED = 'Paused',
  // Maps to PipelineStatus.FAILED
  FAILED = 'Failed',
  // Maps to PipelineStatus.SHUTTING_DOWN
  SHUTTING_DOWN = 'Shutting down …'
}

interface PipelineState {
  clientStatus: Map<string, ClientPipelineStatus>
  setStatus: (id: string, status: ClientPipelineStatus) => void
}

export const usePipelineStateStore = create<PipelineState>()(set => ({
  clientStatus: new Map<string, ClientPipelineStatus>(),
  setStatus: (id: string, status: ClientPipelineStatus) => {
    set(store => {
      const newMap = new Map(store.clientStatus)
      newMap.set(id, status)
      return { clientStatus: newMap }
    })
  }
}))
