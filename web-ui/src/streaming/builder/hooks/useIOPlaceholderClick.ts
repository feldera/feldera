import { NodeProps, useReactFlow } from 'reactflow'
import useDrawerState from './useDrawerState'

// When we click on input or output placeholder, we (a) add a new node at the
// position where the placeholder was, and (b) re-add the placeholder node a bit
// below its former position.
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
