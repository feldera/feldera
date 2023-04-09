import { useCallback } from 'react'
import { useBuilderState } from '../useBuilderState'

// this hook implements the logic for clicking a workflow node
// on workflow node click: create a new child node of the clicked node
function useDebouncedSave() {
  const setSaveState = useBuilderState(state => state.setSaveState)

  const startDebouncing = useCallback(() => {
    setSaveState('isDebouncing')
  }, [setSaveState])

  return startDebouncing
}

export default useDebouncedSave
