// State for the pipeline builder, contains everything we'll eventually send to
// the server for creating a pipeline.

import { SaveIndicatorState } from '$lib/components/common/SaveIndicator'
import { ProgramDescr, RuntimeConfig } from '$lib/types/manager'
import { create } from 'zustand'

interface PipelineBuilderState {
  saveState: SaveIndicatorState
  project: ProgramDescr | undefined
  name: string
  description: string
  config: RuntimeConfig
  setName: (name: string) => void
  setDescription: (description: string) => void
  setSaveState: (saveState: SaveIndicatorState) => void
  setConfig: (config: RuntimeConfig) => void
  setProject: (config: ProgramDescr | undefined) => void
}

export const useBuilderState = create<PipelineBuilderState>(set => ({
  saveState: 'isUpToDate',
  project: undefined,
  name: '',
  description: '',
  config: {},
  setName: (name: string) => set({ name }),
  setDescription: (description: string) => set({ description }),
  setSaveState: (saveState: SaveIndicatorState) => set({ saveState }),
  setProject: (project: ProgramDescr | undefined) => set({ project }),
  setConfig: (config: RuntimeConfig) => set({ config })
}))
