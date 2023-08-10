// What happens when we click either the input or output placeholder.

import { NodeProps, useReactFlow } from 'reactflow'
import useDrawerState from './useDrawerState'

export function useIOPlaceholderClick(id: NodeProps['id']) {
  const drawer = useDrawerState()
  const { getNode } = useReactFlow()

  const onClick = () => {
    // The parent is the placeholder we just clicked
    const parentNode = getNode(id)
    if (!parentNode) {
      return
    }

    // Input or Output?
    const newNodeType = parentNode.id === 'inputPlaceholder' ? 'inputNode' : 'outputNode'
    drawer.open(newNodeType)
  }

  return onClick
}

export default useIOPlaceholderClick
