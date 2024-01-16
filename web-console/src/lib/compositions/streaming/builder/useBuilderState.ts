// State for the pipeline builder, contains everything we'll eventually send to
// the server for creating a pipeline.

import { EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import { create } from 'zustand'

interface FormError {
  name?: { message?: string }
}

type PipelineBuilderState = {
  pipelineName: string // The name of pipeline currently being edited
  setPipelineName: (pipelineName: string) => void
  saveState: EntitySyncIndicatorStatus
  setSaveState: (saveState: EntitySyncIndicatorStatus) => void
  formError: FormError
  setFormError: (formError: FormError) => void
}

export const useBuilderState = create<PipelineBuilderState>(set => ({
  pipelineName: '',
  setPipelineName: (pipelineName: string) => set({ pipelineName }),
  saveState: 'isLoading',
  setSaveState: (saveState: EntitySyncIndicatorStatus) => set({ saveState }),
  formError: {},
  setFormError: (formError: FormError) => set({ formError })
}))
