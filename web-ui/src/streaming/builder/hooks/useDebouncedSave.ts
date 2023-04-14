// Save the pipeline config, but delay the saving so we don't save on every
// change.

import { useCallback } from 'react'
import { useBuilderState } from '../useBuilderState'

function useDebouncedSave() {
  const setSaveState = useBuilderState(state => state.setSaveState)

  const startDebouncing = useCallback(() => {
    setSaveState('isDebouncing')
  }, [setSaveState])

  return startDebouncing
}

export default useDebouncedSave
