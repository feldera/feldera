// Indicates that we want to save the pipeline config, but we also delay the
// saving so we don't save on every change.

import { useCallback } from 'react'
import { useBuilderState } from '../useBuilderState'

function useDebouncedSave() {
  const setSaveState = useBuilderState(state => state.setSaveState)

  const startDebouncing = useCallback(() => {
    // Setting the saveState to isDebouncing will trigger a debounced save of
    // the pipline config. For the logic that does the actual saving check the
    // `pages/streaming/builder/index.tsx` file.
    setSaveState('isDebouncing')
  }, [setSaveState])

  return startDebouncing
}

export default useDebouncedSave
