import { sqlPlaceholderNode } from '$lib/components/streaming/builder/PipelineBuilder'
import { Node, NodeProps, ReactFlowInstance, useReactFlow } from 'reactflow'

import { useBuilderState } from './useBuilderState'
import useDebouncedSave from './useDebouncedSave'

export const useDeleteNodeProgram = () => {
  const savePipeline = useDebouncedSave()
  const setProject = useBuilderState(state => state.setProject)
  return ({ addNodes, deleteElements, getEdges }: ReactFlowInstance, parentNode: Node) => {
    setProject(undefined)

    // if we delete the program then all edges are affected
    deleteElements({ edges: getEdges() })
    savePipeline()

    // If we deleted the SQL program, add the placeholder back but at the same
    // position as the deleted node, make sure to reset the dimensions
    // otherwise it will look wrong
    addNodes({ ...sqlPlaceholderNode, width: undefined, height: undefined, position: parentNode.position })
  }
}

// Logic that runs when we remove a node from the graph. Also puts back the
// sqlPlaceholder if we removed the program node, and drops all edges that were
// attached to the node.
export const useDeleteNode =
  (onDelete: (reactFlow: ReactFlowInstance, parentNode: Node) => void) => (id: NodeProps['id']) => {
    /* eslint-disable react-hooks/rules-of-hooks */
    const reactFlow = useReactFlow()
    const { getNode, deleteElements } = reactFlow

    const onClick = () => {
      const parentNode = getNode(id)
      if (!parentNode) {
        return
      }
      // Just hide the node(s) for now (see issue below):
      parentNode.hidden = true

      // TODO: find a better way / file a bug report
      //
      // The timer is a hack around a reactflow bug to make sure the node is
      // deleted only after the click event is finished. If we don't do this,
      // reactflow throws an error since it seems to handle some other onClick
      // handlers referencing that node. This problem seems to happen because the
      // delete button is on the node itself. It might be an alternative to just
      // have the delete button somewhere else.
      const timer = setTimeout(() => {
        deleteElements({ nodes: [parentNode] })
        onDelete(reactFlow, parentNode)
      }, 50)

      return () => clearTimeout(timer)
    }

    return onClick
  }
