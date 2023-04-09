import { ProjectWithSchema } from 'src/types/program'
import { SaveIndicatorState } from 'src/views/utils/SaveIndicator'
import { create } from 'zustand'

interface BuilderState {
  saveState: SaveIndicatorState
  project: ProjectWithSchema | undefined
  name: string
  description: string
  config: string
  setName: (name: string) => void
  setDescription: (description: string) => void
  setSaveState: (saveState: SaveIndicatorState) => void
  setConfig: (config: string) => void
  setProject: (config: ProjectWithSchema | undefined) => void
}

export const useBuilderState = create<BuilderState>(set => ({
  saveState: 'isUpToDate',
  project: undefined,
  name: '',
  description: '',
  config: 'inputs:\n    ${inputs}\noutputs:\n    ${outputs}',
  setName: (name: string) => set({ name }),
  setDescription: (description: string) => set({ description }),
  setSaveState: (saveState: SaveIndicatorState) => set({ saveState }),
  setProject: (project: ProjectWithSchema | undefined) => set({ project }),
  setConfig: (config: string) => set({ config })
}))
